/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.service;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.SyncUtil;

/**
 * Records that the tokens of this node were changed by {@link StorageService#shrinkTokens}, so that the node can
 * restart with saved tokens whose count differs from {@code num_tokens} until the configuration is updated.
 * <p>
 * The record is a small file in the metadata directory, outside the system tables so that no schema change is needed
 * and older versions can still read the data directories. It holds:
 * <ul>
 *     <li>the token sets the node may have saved in {@code system.local}: the new set, and the previous one, since
 *     the record is written (and synced) before {@code system.local} is updated and the node can stop in between;
 *     </li>
 *     <li>the values of {@code num_tokens} the node may be configured with: every value configured when a shrink ran
 *     and every token count the node had, so that a configuration updated between two rounds of shrinks is accepted,
 *     but not an unrelated value.</li>
 * </ul>
 * The record is removed when the node starts with {@code num_tokens} matching its tokens.
 */
public final class TokenCountOverride
{
    private static final Logger logger = LoggerFactory.getLogger(TokenCountOverride.class);

    @VisibleForTesting
    static final String FILE_NAME = "token_count_override";
    private static final String TOKEN_SETS = "token_sets";
    private static final String NUM_TOKENS = "num_tokens";

    private TokenCountOverride()
    {
    }

    @VisibleForTesting
    static File file()
    {
        return new File(DatabaseDescriptor.getMetadataDirectory(), FILE_NAME);
    }

    private static final class Record
    {
        final Set<String> tokenSets = new LinkedHashSet<>();
        final Set<String> numTokens = new LinkedHashSet<>();
    }

    /**
     * Records that the node is about to replace its {@code previous} tokens with {@code kept}, atomically replacing a
     * previous record and syncing it to disk.
     *
     * @param numTokens the configured {@code num_tokens}
     */
    public static void record(Collection<Token> previous, Collection<Token> kept, int numTokens)
    {
        Record record = new Record();
        Record existing = read();
        if (existing == null && file().exists())
            throw new FSWriteError(new IOException("the token count override is unreadable, it would lose the num_tokens values of the previous shrinks; check or remove it"), file());
        if (existing != null && existing.tokenSets.contains(tokenSet(previous)))
            record.numTokens.addAll(existing.numTokens); // a previous round
        record.numTokens.add(Integer.toString(numTokens));
        record.numTokens.add(Integer.toString(previous.size()));
        record.tokenSets.add(tokenSet(previous));
        record.tokenSets.add(tokenSet(kept));
        write(record);
    }

    /**
     * The shrink from {@code previous} to {@code kept} failed before the new tokens were saved: the saved tokens are
     * still {@code previous}. The record is kept only if {@code previous} still needs it.
     */
    public static void rollback(Collection<Token> previous, Collection<Token> kept)
    {
        Record record = read();
        if (record == null)
            return;
        record.tokenSets.remove(tokenSet(kept));
        if (record.tokenSets.isEmpty() || previous.size() == DatabaseDescriptor.getNumTokens())
            clear();
        else
            write(record);
    }

    /**
     * @return true if the tokens are ones a shrink may have saved, and {@code num_tokens} is a value the record
     * accepts
     */
    public static boolean matches(Collection<Token> tokens, int numTokens)
    {
        Record record = read();
        return record != null
               && record.tokenSets.contains(tokenSet(tokens))
               && record.numTokens.contains(Integer.toString(numTokens));
    }

    /**
     * Removes the record, once {@code num_tokens} matches the tokens of the node again.
     */
    public static void clear()
    {
        File file = file();
        if (file.exists() && !file.tryDelete())
            logger.warn("Could not delete {}", file);
    }

    public static boolean exists()
    {
        return file().exists();
    }

    private static Record read()
    {
        File file = file();
        if (!file.exists())
            return null;
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8))
        {
            properties.load(reader);
        }
        catch (IOException | IllegalArgumentException e)
        {
            logger.warn("Cannot read the token count override {}", file, e);
            return null;
        }
        Record record = new Record();
        record.tokenSets.addAll(split(properties.getProperty(TOKEN_SETS)));
        record.numTokens.addAll(split(properties.getProperty(NUM_TOKENS)));
        return record;
    }

    private static List<String> split(String value)
    {
        return value == null || value.isEmpty() ? new ArrayList<>() : Arrays.asList(value.split(","));
    }

    private static void write(Record record)
    {
        Properties properties = new Properties();
        properties.setProperty(TOKEN_SETS, String.join(",", record.tokenSets));
        properties.setProperty(NUM_TOKENS, String.join(",", record.numTokens));
        File target = file();
        File tmp = new File(target.parent(), FILE_NAME + ".tmp");
        try
        {
            StringWriter content = new StringWriter();
            properties.store(content, "Written by nodetool settokens: the number of tokens of this node differs from num_tokens. Update num_tokens in cassandra.yaml.");
            Files.createDirectories(target.parent().toPath());
            try (FileChannel channel = FileChannel.open(tmp.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING))
            {
                ByteBuffer buffer = ByteBuffer.wrap(content.toString().getBytes(StandardCharsets.UTF_8));
                while (buffer.hasRemaining())
                    channel.write(buffer);
                channel.force(true);
            }
            tmp.move(target);
            SyncUtil.trySyncDir(target.parent());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, target);
        }
    }

    /** A token set as {@code <count>:<sha256 of the sorted tokens>}. */
    @VisibleForTesting
    static String tokenSet(Collection<Token> tokens)
    {
        List<Token> sorted = new ArrayList<>(tokens);
        sorted.sort(Token::compareTo);
        try
        {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (Token token : sorted)
            {
                digest.update(token.toString().getBytes(StandardCharsets.UTF_8));
                digest.update((byte) ',');
            }
            return tokens.size() + ":" + Hex.bytesToHex(digest.digest());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
    }
}
