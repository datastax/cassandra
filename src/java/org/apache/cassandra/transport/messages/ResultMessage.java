/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport.messages;


import java.util.function.UnaryOperator;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.MD5Digest;

public abstract class ResultMessage<T extends ResultMessage<T>> extends Message.Response
{
    public static final Message.Codec<ResultMessage> codec = new Message.Codec<ResultMessage>()
    {
        public ResultMessage decode(ByteBuf body, ProtocolVersion version)
        {
            Kind kind = Kind.fromId(body.readInt());
            return kind.subcodec.decode(body, version);
        }

        public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            dest.writeInt(msg.kind.id);
            msg.kind.subcodec.encode(msg, dest, version);
        }

        public int encodedSize(ResultMessage msg, ProtocolVersion version)
        {
            return 4 + msg.kind.subcodec.encodedSize(msg, version);
        }
    };

    public enum Kind
    {

        VOID               (1, Void.subcodec),
        ROWS               (2, Rows.subcodec),
        SET_KEYSPACE       (3, SetKeyspace.subcodec),
        PREPARED           (4, Prepared.subcodec),
        SCHEMA_CHANGE      (5, SchemaChange.subcodec);
        public final int id;
        public final Message.Codec<ResultMessage> subcodec;

        private static final Kind[] ids;
        static
        {
            int maxId = -1;
            for (Kind k : Kind.values())
                maxId = Math.max(maxId, k.id);
            ids = new Kind[maxId + 1];
            for (Kind k : Kind.values())
            {
                if (ids[k.id] != null)
                    throw new IllegalStateException("Duplicate kind id");
                ids[k.id] = k;
            }
        }

        private Kind(int id, Message.Codec<ResultMessage> subcodec)
        {
            this.id = id;
            this.subcodec = subcodec;
        }

        public static Kind fromId(int id)
        {
            Kind k = ids[id];
            if (k == null)
                throw new ProtocolException(String.format("Unknown kind id %d in RESULT message", id));
            return k;
        }
    }

    public final Kind kind;

    protected ResultMessage(Kind kind)
    {
        super(Message.Type.RESULT);
        this.kind = kind;
    }

    public T withOverriddenKeyspace(UnaryOperator<String> keyspaceMapper)
    {
        return (T) this;
    }

    public static class Void extends ResultMessage<Void>
    {
        // Even though we have no specific information here, don't make a
        // singleton since as each message it has in fact a streamid and connection.
        public Void()
        {
            super(Kind.VOID);
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ByteBuf body, ProtocolVersion version)
            {
                return new Void();
            }

            public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version)
            {
                assert msg instanceof Void;
            }

            public int encodedSize(ResultMessage msg, ProtocolVersion version)
            {
                return 0;
            }
        };

        @Override
        public String toString()
        {
            return "EMPTY RESULT";
        }
    }

    public static class SetKeyspace extends ResultMessage<SetKeyspace>
    {
        public final String keyspace;

        public SetKeyspace(String keyspace)
        {
            super(Kind.SET_KEYSPACE);
            this.keyspace = keyspace;
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ByteBuf body, ProtocolVersion version)
            {
                String keyspace = CBUtil.readString(body);
                return new SetKeyspace(keyspace);
            }

            public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version)
            {
                assert msg instanceof SetKeyspace;
                CBUtil.writeAsciiString(((SetKeyspace)msg).keyspace, dest);
            }

            public int encodedSize(ResultMessage msg, ProtocolVersion version)
            {
                assert msg instanceof SetKeyspace;
                return CBUtil.sizeOfAsciiString(((SetKeyspace)msg).keyspace);
            }
        };

        @Override
        public String toString()
        {
            return "RESULT set keyspace " + keyspace;
        }

        @Override
        public SetKeyspace withOverriddenKeyspace(UnaryOperator<String> keyspaceMapper)
        {
            if (keyspaceMapper == Constants.IDENTITY_STRING_MAPPER)
                return this;

            String newKeyspaceName = keyspaceMapper.apply(keyspace);
            if (keyspace.equals(newKeyspaceName))
                return this;

            SetKeyspace r = new SetKeyspace(newKeyspaceName);
            r.setWarnings(getWarnings());
            r.setCustomPayload(getCustomPayload());
            r.setSource(getSource());
            r.setStreamId(getStreamId());
            return r;
        }
    }

    public static class Rows extends ResultMessage<Rows>
    {
        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ByteBuf body, ProtocolVersion version)
            {
                return new Rows(ResultSet.codec.decode(body, version));
            }

            public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version)
            {
                assert msg instanceof Rows;
                Rows rowMsg = (Rows)msg;
                ResultSet.codec.encode(rowMsg.result, dest, version);
            }

            public int encodedSize(ResultMessage msg, ProtocolVersion version)
            {
                assert msg instanceof Rows;
                Rows rowMsg = (Rows)msg;
                return ResultSet.codec.encodedSize(rowMsg.result, version);
            }
        };

        public final ResultSet result;

        public Rows(ResultSet result)
        {
            super(Kind.ROWS);
            this.result = result;
        }

        @Override
        public String toString()
        {
            return "ROWS " + result;
        }

        @Override
        public Rows withOverriddenKeyspace(UnaryOperator<String> keyspaceMapper)
        {
            if (keyspaceMapper == Constants.IDENTITY_STRING_MAPPER)
                return this;

            return withResultSet(result.withOverriddenKeyspace(keyspaceMapper));
        }

        public Rows withResultSet(ResultSet newResultSet)
        {
            if (newResultSet == result)
                return this;

            Rows r = new Rows(newResultSet);
            r.setWarnings(getWarnings());
            r.setCustomPayload(getCustomPayload());
            r.setSource(getSource());
            r.setStreamId(getStreamId());
            return r;
        }
    }

    public static class Prepared extends ResultMessage<Prepared>
    {
        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ByteBuf body, ProtocolVersion version)
            {
                MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
                MD5Digest resultMetadataId = null;
                if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                    resultMetadataId = MD5Digest.wrap(CBUtil.readBytes(body));
                ResultSet.PreparedMetadata metadata = ResultSet.PreparedMetadata.codec.decode(body, version);

                ResultSet.ResultMetadata resultMetadata = ResultSet.ResultMetadata.EMPTY;
                if (version.isGreaterThan(ProtocolVersion.V1))
                    resultMetadata = ResultSet.ResultMetadata.codec.decode(body, version);

                return new Prepared(id, resultMetadataId, metadata, resultMetadata);
            }

            public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version)
            {
                assert msg instanceof Prepared;
                Prepared prepared = (Prepared)msg;
                assert prepared.statementId != null;

                CBUtil.writeBytes(prepared.statementId.bytes, dest);
                if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                    CBUtil.writeBytes(prepared.resultMetadataId.bytes, dest);

                ResultSet.PreparedMetadata.codec.encode(prepared.metadata, dest, version);
                if (version.isGreaterThan(ProtocolVersion.V1))
                    ResultSet.ResultMetadata.codec.encode(prepared.resultMetadata, dest, version);
            }

            public int encodedSize(ResultMessage msg, ProtocolVersion version)
            {
                assert msg instanceof Prepared;
                Prepared prepared = (Prepared)msg;
                assert prepared.statementId != null;

                int size = 0;
                size += CBUtil.sizeOfBytes(prepared.statementId.bytes);
                if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                    size += CBUtil.sizeOfBytes(prepared.resultMetadataId.bytes);
                size += ResultSet.PreparedMetadata.codec.encodedSize(prepared.metadata, version);
                if (version.isGreaterThan(ProtocolVersion.V1))
                    size += ResultSet.ResultMetadata.codec.encodedSize(prepared.resultMetadata, version);
                return size;
            }
        };

        public final MD5Digest statementId;
        public final MD5Digest resultMetadataId;

        /** Describes the variables to be bound in the prepared statement */
        public final ResultSet.PreparedMetadata metadata;

        /** Describes the results of executing this prepared statement */
        public final ResultSet.ResultMetadata resultMetadata;

        public Prepared(MD5Digest statementId, MD5Digest resultMetadataId, ResultSet.PreparedMetadata metadata, ResultSet.ResultMetadata resultMetadata)
        {
            super(Kind.PREPARED);
            this.statementId = statementId;
            this.resultMetadataId = resultMetadataId;
            this.metadata = metadata;
            this.resultMetadata = resultMetadata;
        }

        @VisibleForTesting
        public Prepared withResultMetadata(ResultSet.ResultMetadata resultMetadata)
        {
            return new Prepared(statementId, resultMetadata.getResultMetadataId(), metadata, resultMetadata);
        }

        @Override
        public Prepared withOverriddenKeyspace(UnaryOperator<String> keyspaceMapper)
        {
            if (keyspaceMapper == Constants.IDENTITY_STRING_MAPPER)
                return this;

            ResultSet.PreparedMetadata newPreparedMetadata = metadata.withOverriddenKeyspace(keyspaceMapper);
            ResultSet.ResultMetadata newResultSetMetadata = resultMetadata.withOverriddenKeyspace(keyspaceMapper);
            if (newPreparedMetadata == metadata && newResultSetMetadata == resultMetadata)
                return this;

            Prepared r = new Prepared(statementId,
                                      resultMetadataId,
                                      newPreparedMetadata,
                                      newResultSetMetadata);
            r.setWarnings(getWarnings());
            r.setCustomPayload(getCustomPayload());
            r.setSource(getSource());
            r.setStreamId(getStreamId());
            return r;
        }

        @Override
        public String toString()
        {
            return "RESULT PREPARED " + statementId + " " + metadata + " (resultMetadata=" + resultMetadata + ")";
        }
    }

    public static class SchemaChange extends ResultMessage<SchemaChange>
    {
        public final Event.SchemaChange change;

        public SchemaChange(Event.SchemaChange change)
        {
            super(Kind.SCHEMA_CHANGE);
            this.change = change;
        }

        public static final Message.Codec<ResultMessage> subcodec = new Message.Codec<ResultMessage>()
        {
            public ResultMessage decode(ByteBuf body, ProtocolVersion version)
            {
                return new SchemaChange(Event.SchemaChange.deserializeEvent(body, version));
            }

            public void encode(ResultMessage msg, ByteBuf dest, ProtocolVersion version)
            {
                assert msg instanceof SchemaChange;
                SchemaChange scm = (SchemaChange)msg;
                scm.change.serializeEvent(dest, version);
            }

            public int encodedSize(ResultMessage msg, ProtocolVersion version)
            {
                assert msg instanceof SchemaChange;
                SchemaChange scm = (SchemaChange)msg;
                return scm.change.eventSerializedSize(version);
            }
        };

        @Override
        public SchemaChange withOverriddenKeyspace(UnaryOperator<String> keyspaceMapper)
        {
            if (keyspaceMapper == Constants.IDENTITY_STRING_MAPPER)
                return this;

            Event.SchemaChange newEvent = change.withOverriddenKeyspace(keyspaceMapper);
            if (change == newEvent)
                return this;

            SchemaChange r = new SchemaChange(newEvent);
            r.setWarnings(getWarnings());
            r.setCustomPayload(getCustomPayload());
            r.setSource(getSource());
            r.setStreamId(getStreamId());
            return r;
        }

        @Override
        public String toString()
        {
            return "RESULT schema change " + change;
        }
    }
}
