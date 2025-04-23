/*
 * Copyright DataStax, Inc.
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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.utils.FBUtilities;

/**
 * {@code SELECT} query options for ANN search.
 */
public class ANNOptions
{
    public static final String RERANK_K_OPTION_NAME = "rerank_k";
    public static final String USE_PRUNING_OPTION_NAME = "use_pruning";

    public static final ANNOptions NONE = new ANNOptions(null, null);

    public static final Serializer serializer = new Serializer();

    /**
     * The amplified limit for the ANN query to get more accurate results.
     * A value lesser or equals to zero means no reranking.
     * A {@code null} value means the option is not present.
     */
    @Nullable
    public final Integer rerankK;

    /**
     * Whether to use pruning to speed up the ANN search. If {@code null}, the default value is used.
     */
    @Nullable
    public final Boolean usePruning;

    private ANNOptions(@Nullable Integer rerankK, @Nullable Boolean usePruning)
    {
        this.rerankK = rerankK;
        this.usePruning = usePruning;
    }

    public static ANNOptions create(@Nullable Integer rerankK, @Nullable Boolean usePruning)
    {
        // if all the options are null, return the NONE instance
        return rerankK == null && usePruning == null ? NONE : new ANNOptions(rerankK, usePruning);
    }

    /**
     * Validates the ANN options by checking that they are within the guardrails and that peers support the options.
     */
    public void validate(ClientState state, String keyspace, int limit)
    {
        if (rerankK == null && usePruning == null)
            return;

        if (rerankK != null)
        {
            if (rerankK > 0 && rerankK < limit)
                throw new InvalidRequestException(String.format("Invalid rerank_k value %d greater than 0 and less than limit %d", rerankK, limit));

            Guardrails.annRerankKMaxValue.guard(rerankK, "ANN options", false, state);
        }

        // Ensure that all nodes in the cluster are in a version that supports ANN options, including this one
        assert keyspace != null;
        Set<InetAddressAndPort> badNodes = MessagingService.instance().endpointsWithConnectionsOnVersionBelow(keyspace, MessagingService.VERSION_DS_11);
        if (MessagingService.current_version < MessagingService.VERSION_DS_11)
            badNodes.add(FBUtilities.getBroadcastAddressAndPort());
        if (!badNodes.isEmpty())
            throw new InvalidRequestException("ANN options are not supported in clusters below DS 11.");
    }

    /**
     * Returns the ANN options stored the given map of options.
     *
     * @param map the map of options in the {@code WITH ANN_OPTION} of a {@code SELECT} query
     * @return the ANN options in the specified {@code SELECT} options, or {@link #NONE} if no options are present
     */
    public static ANNOptions fromMap(Map<String, String> map)
    {
        Integer rerankK = null;
        Boolean usePruning = null;

        for (Map.Entry<String, String> entry : map.entrySet())
        {
            String name = entry.getKey();
            String value = entry.getValue();

            if (name.equals(RERANK_K_OPTION_NAME))
            {
                rerankK = parseRerankK(value);
            }
            else if (name.equals(USE_PRUNING_OPTION_NAME))
            {
                usePruning = parseUsePruning(value);
            }
            else
            {
                throw new InvalidRequestException("Unknown ANN option: " + name);
            }
        }

        return ANNOptions.create(rerankK, usePruning);
    }

    private static int parseRerankK(String value)
    {
        int rerankK;

        try
        {
            rerankK = Integer.parseInt(value);
        }
        catch (NumberFormatException e)
        {
            throw new InvalidRequestException(String.format("Invalid '%s' ANN option. Expected a positive int but found: %s",
                                                            RERANK_K_OPTION_NAME, value));
        }

        return rerankK;
    }

    private static boolean parseUsePruning(String value)
    {
        value = value.toLowerCase();
        if (!value.equals("true") && !value.equals("false"))
            throw new InvalidRequestException(String.format("Invalid '%s' ANN option. Expected a boolean but found: %s",
                                                            USE_PRUNING_OPTION_NAME, value));
        return Boolean.parseBoolean(value);
    }

    public String toCQLString()
    {
        StringBuilder sb = new StringBuilder("{");
        if (rerankK != null)
            sb.append(String.format("'%s': %d", RERANK_K_OPTION_NAME, rerankK));
        if (usePruning != null)
        {
            if (rerankK != null)
                sb.append(", ");
            sb.append(String.format("'%s': %b", USE_PRUNING_OPTION_NAME, usePruning));
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ANNOptions that = (ANNOptions) o;
        return Objects.equals(rerankK, that.rerankK) &&
               Objects.equals(usePruning, that.usePruning);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rerankK, usePruning);
    }

    /**
     * Serializer for {@link ANNOptions}.
     * </p>
     * This serializer writes an int containing bit flags that indicate which options are present, allowing the future
     * addition of new options without increasing the messaging version. We should be able to create compatible messages
     * in the future if we add new options and those are not explicitly set in the user query. If we receive a message
     * with unknown newer options from a newer node, we will reject it.
     * </p>
     * This approach should be more space-efficient than simply using a map, as we do with the index creation options.
     * Space is more important in this case because the {@link ANNOptions} are sent with every {@code SELECT} query. The
     * downside is that we only allow for up to 32 options, which seems reasonable. If we ever need more options, we can
     * use the last bit flag to indicate that we need to read more flags from the input.
     */
    public static class Serializer
    {
        /** Bit flags mask to check if the rerank K option is present. */
        private static final int RERANK_K_MASK = 1;
        private static final int USE_PRUNING_MASK = 2;
        /** Bit flags mask to check if there are any unknown options. It's the negation of all the known flags. */
        private static final int UNKNOWN_OPTIONS_MASK = ~(RERANK_K_MASK | USE_PRUNING_MASK);

        /*
         * If you add a new option, then update ANNOptionsTest.FutureANNOptions and possibly add a new test verifying
         * that the serialization of the updated and original versions of the options are compatible.
         */

        public void serialize(ANNOptions options, DataOutputPlus out, int version) throws IOException
        {
            // ANN options are only supported in DS 11 and above, so don't serialize anything if the messaging version is lower
            if (version < MessagingService.VERSION_DS_11)
            {
                if (options != NONE)
                    throw new IllegalStateException("Unable to serialize ANN options with messaging version: " + version);
                return;
            }

            int flags = flags(options);
            out.writeInt(flags);

            if (options.rerankK != null)
                out.writeUnsignedVInt32(options.rerankK);
            if (options.usePruning != null)
                out.writeBoolean(options.usePruning);
        }

        public ANNOptions deserialize(DataInputPlus in, int version) throws IOException
        {
            // ANN options are only supported in DS 11 and above, so don't read anything if the messaging version is lower
            if (version < MessagingService.VERSION_DS_11)
                return ANNOptions.NONE;

            int flags = in.readInt();

            // Reject any flags for unknown options that may have been written by a node running newer code.
            if ((flags & UNKNOWN_OPTIONS_MASK) != 0)
                throw new IOException("Found unsupported ANN options, likely due to the ANN options containing " +
                                      "new options that are not supported by this node.");

            Integer rerankK = hasRerankK(flags) ? (int) in.readUnsignedVInt() : null;
            Boolean usePruning = hasUsePruning(flags) ? in.readBoolean() : null;

            return ANNOptions.create(rerankK, usePruning);
        }

        public long serializedSize(ANNOptions options, int version)
        {
            // ANN options are only supported in DS 11 and above, so no size if the messaging version is lower
            if (version < MessagingService.VERSION_DS_11)
                return 0;

            int flags = flags(options);
            long size = TypeSizes.sizeof(flags);

            if (options.rerankK != null)
                size += TypeSizes.sizeofUnsignedVInt(options.rerankK);
            if (options.usePruning != null)
                size += TypeSizes.sizeof(options.usePruning);

            return size;
        }

        private static int flags(ANNOptions options)
        {
            int flags = 0;

            if (options == NONE)
                return flags;

            if (options.rerankK != null)
                flags |= RERANK_K_MASK;
            if (options.usePruning != null)
                flags |= USE_PRUNING_MASK;

            return flags;
        }

        private static boolean hasRerankK(int flags)
        {
            return (flags & RERANK_K_MASK) == RERANK_K_MASK;
        }

        private static boolean hasUsePruning(int flags)
        {
            return (flags & USE_PRUNING_MASK) == USE_PRUNING_MASK;
        }
    }
}
