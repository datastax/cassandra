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
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Hex;

/**
 * Records that the tokens of this node were changed by {@link StorageService#shrinkTokens}, so that the node can
 * restart with saved tokens whose count differs from {@code num_tokens} until the configuration is updated.
 * <p>
 * The record is a small file in the metadata directory with the number of tokens, a checksum of the token set and the
 * {@code num_tokens} configured when the tokens were changed: the node accepts its saved tokens while {@code num_tokens}
 * is still that value (the configuration was not updated yet), not if {@code num_tokens} was changed to yet another
 * value. It is written before the new tokens are saved in {@code system.local}: if the node stops in between, the saved
 * tokens still match {@code num_tokens} and the record is ignored (and removed) at the next start. It lives outside the
 * system tables so that no schema change is needed and older versions can still read the data directories.
 */
public final class TokenCountOverride
{
    @VisibleForTesting
    static final String FILE_NAME = "token_count_override";
    private static final String COUNT = "token_count";
    private static final String CHECKSUM = "tokens_sha256";
    private static final String NUM_TOKENS = "num_tokens";

    private TokenCountOverride()
    {
    }

    @VisibleForTesting
    static File file()
    {
        return new File(DatabaseDescriptor.getMetadataDirectory(), FILE_NAME);
    }

    /**
     * Records the token set the node is about to save, atomically replacing a previous record.
     *
     * @param numTokens the configured {@code num_tokens}
     */
    public static void record(Collection<Token> tokens, int numTokens)
    {
        Properties properties = new Properties();
        properties.setProperty(COUNT, Integer.toString(tokens.size()));
        properties.setProperty(CHECKSUM, checksum(tokens));
        properties.setProperty(NUM_TOKENS, Integer.toString(numTokens));
        File target = file();
        File tmp = new File(target.parent(), FILE_NAME + ".tmp");
        try
        {
            Files.createDirectories(target.parent().toPath());
            try (Writer writer = Files.newBufferedWriter(tmp.toPath(), StandardCharsets.UTF_8))
            {
                properties.store(writer, "Written by nodetool settokens: the number of tokens of this node differs from num_tokens. Update num_tokens in cassandra.yaml.");
            }
            tmp.move(target);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, target);
        }
    }

    /**
     * @return true if the tokens are the ones recorded by the last shrink, and {@code num_tokens} is still the value
     * configured at that time
     */
    public static boolean matches(Collection<Token> tokens, int numTokens)
    {
        File file = file();
        if (!file.exists())
            return false;
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(file.toPath(), StandardCharsets.UTF_8))
        {
            properties.load(reader);
        }
        catch (IOException | IllegalArgumentException e)
        {
            return false;
        }
        return Integer.toString(tokens.size()).equals(properties.getProperty(COUNT))
               && checksum(tokens).equals(properties.getProperty(CHECKSUM))
               && Integer.toString(numTokens).equals(properties.getProperty(NUM_TOKENS));
    }

    /**
     * Removes the record, once {@code num_tokens} matches the tokens of the node again.
     */
    public static void clear()
    {
        file().tryDelete();
    }

    public static boolean exists()
    {
        return file().exists();
    }

    @VisibleForTesting
    static String checksum(Collection<Token> tokens)
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
            return Hex.bytesToHex(digest.digest());
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
    }
}
