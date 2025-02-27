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
package org.apache.cassandra.transport;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.ReflectionUtils;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A message from the CQL binary protocol.
 */
public abstract class Message
{
    protected static final Logger logger = LoggerFactory.getLogger(Message.class);

    public interface Codec<M extends Message> extends CBCodec<M> {}

    public enum Direction
    {
        REQUEST, RESPONSE;

        public static Direction extractFromVersion(int versionWithDirection)
        {
            return (versionWithDirection & 0x80) == 0 ? REQUEST : RESPONSE;
        }

        public int addToVersion(int rawVersion)
        {
            return this == REQUEST ? (rawVersion & 0x7F) : (rawVersion | 0x80);
        }
    }

    public enum Type
    {
        ERROR          (0,  Direction.RESPONSE, ErrorMessage.codec),
        STARTUP        (1,  Direction.REQUEST,  StartupMessage.codec),
        READY          (2,  Direction.RESPONSE, ReadyMessage.codec),
        AUTHENTICATE   (3,  Direction.RESPONSE, AuthenticateMessage.codec),
        CREDENTIALS    (4,  Direction.REQUEST,  UnsupportedMessageCodec.instance),
        OPTIONS        (5,  Direction.REQUEST,  OptionsMessage.codec),
        SUPPORTED      (6,  Direction.RESPONSE, SupportedMessage.codec),
        QUERY          (7,  Direction.REQUEST,  QueryMessage.codec),
        RESULT         (8,  Direction.RESPONSE, ResultMessage.codec),
        PREPARE        (9,  Direction.REQUEST,  PrepareMessage.codec),
        EXECUTE        (10, Direction.REQUEST,  ExecuteMessage.codec),
        REGISTER       (11, Direction.REQUEST,  RegisterMessage.codec),
        EVENT          (12, Direction.RESPONSE, EventMessage.codec),
        BATCH          (13, Direction.REQUEST,  BatchMessage.codec),
        AUTH_CHALLENGE (14, Direction.RESPONSE, AuthChallenge.codec),
        AUTH_RESPONSE  (15, Direction.REQUEST,  AuthResponse.codec),
        AUTH_SUCCESS   (16, Direction.RESPONSE, AuthSuccess.codec);

        public final int opcode;
        public final Direction direction;
        public final Codec<?> codec;

        private static final Type[] opcodeIdx;
        static
        {
            int maxOpcode = -1;
            for (Type type : Type.values())
                maxOpcode = Math.max(maxOpcode, type.opcode);
            opcodeIdx = new Type[maxOpcode + 1];
            for (Type type : Type.values())
            {
                if (opcodeIdx[type.opcode] != null)
                    throw new IllegalStateException("Duplicate opcode");
                opcodeIdx[type.opcode] = type;
            }
        }

        Type(int opcode, Direction direction, Codec<?> codec)
        {
            this.opcode = opcode;
            this.direction = direction;
            this.codec = codec;
        }

        public static Type fromOpcode(int opcode, Direction direction)
        {
            if (opcode >= opcodeIdx.length)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            Type t = opcodeIdx[opcode];
            if (t == null)
                throw new ProtocolException(String.format("Unknown opcode %d", opcode));
            if (t.direction != direction)
                throw new ProtocolException(String.format("Wrong protocol direction (expected %s, got %s) for opcode %d (%s)",
                                                          t.direction,
                                                          direction,
                                                          opcode,
                                                          t));
            return t;
        }

        @VisibleForTesting
        public Codec<?> unsafeSetCodec(Codec<?> codec) throws NoSuchFieldException, IllegalAccessException
        {
            Codec<?> original = this.codec;
            Field field = Type.class.getDeclaredField("codec");
            field.setAccessible(true);
            Field modifiers = ReflectionUtils.getField(Field.class, "modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(this, codec);
            return original;
        }
    }

    public final Type type;
    protected Connection connection;
    private int streamId;
    private Envelope source;
    private Map<String, ByteBuffer> customPayload;
    protected ProtocolVersion forcedProtocolVersion = null;

    protected Message(Type type)
    {
        this.type = type;
    }

    public void attach(Connection connection)
    {
        this.connection = connection;
    }

    public Connection connection()
    {
        return connection;
    }

    public Message setStreamId(int streamId)
    {
        this.streamId = streamId;
        return this;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public void setSource(Envelope source)
    {
        this.source = source;
    }

    public Envelope getSource()
    {
        return source;
    }

    public Map<String, ByteBuffer> getCustomPayload()
    {
        return customPayload;
    }

    public void setCustomPayload(Map<String, ByteBuffer> customPayload)
    {
        this.customPayload = customPayload;
    }

    public String debugString()
    {
        return String.format("(%s:%s:%s)", type, streamId, connection == null ? "null" :  connection.getVersion().asInt());
    }

    public static abstract class Request extends Message
    {
        private boolean tracingRequested;
        private final long creationTimeNanos = MonotonicClock.approxTime.now();

        protected Request(Type type)
        {
            super(type);

            if (type.direction != Direction.REQUEST)
                throw new IllegalArgumentException();
        }

        protected boolean isTraceable()
        {
            return false;
        }

        /**
         * Returns the time elapsed since this request was created. Note that this is the total lifetime of the request
         * in the system, so we expect increasing returned values across multiple calls to elapsedTimeSinceCreation.
         *
         * @param timeUnit the time unit in which to return the elapsed time
         * @return the time elapsed since this request was created
         */
        protected long elapsedTimeSinceCreation(TimeUnit timeUnit)
        {
            return timeUnit.convert(MonotonicClock.approxTime.now() - creationTimeNanos, TimeUnit.NANOSECONDS);
        }

        protected abstract CompletableFuture<Response> maybeExecuteAsync(QueryState queryState, long queryStartNanoTime, boolean traceRequest);

        public final CompletableFuture<Response> execute(QueryState queryState, long queryStartNanoTime)
        {
            // at the time of the check, this is approximately the time spent in the NTR stage's queue
            long elapsedTimeSinceCreation = elapsedTimeSinceCreation(TimeUnit.NANOSECONDS);
            ClientMetrics.instance.recordQueueTime(elapsedTimeSinceCreation, TimeUnit.NANOSECONDS);
            if (elapsedTimeSinceCreation > DatabaseDescriptor.getNativeTransportTimeout(TimeUnit.NANOSECONDS))
            {
                ClientMetrics.instance.markTimedOutBeforeProcessing();
                return CompletableFuture.completedFuture(ErrorMessage.fromException(new OverloadedException("Query timed out before it could start")));
            }

            boolean shouldTrace = false;
            UUID tracingSessionId = null;

            if (isTraceable())
            {
                if (isTracingRequested())
                {
                    shouldTrace = true;
                    tracingSessionId = UUIDGen.getTimeUUID();
                    Tracing.instance.newSession(queryState.getClientState(), tracingSessionId, getCustomPayload());
                }
                else if (StorageService.instance.shouldTraceProbablistically())
                {
                    shouldTrace = true;
                    Tracing.instance.newSession(queryState.getClientState(), getCustomPayload());
                }
            }

            Tracing.trace("Initialized tracing in execute. Already elapsed {} ns", (System.nanoTime() - queryStartNanoTime));
            boolean finalShouldTrace = shouldTrace;
            UUID finalTracingSessionId = tracingSessionId;
            return maybeExecuteAsync(queryState, queryStartNanoTime, shouldTrace)
                   .whenComplete((result, ignored) -> {
                       if (finalShouldTrace)
                           Tracing.instance.stopSession();

                       if (isTraceable() && isTracingRequested())
                           result.setTracingId(finalTracingSessionId);
                   });
        }

        public void setTracingRequested()
        {
            tracingRequested = true;
        }

        boolean isTracingRequested()
        {
            return tracingRequested;
        }
    }

    public static abstract class Response extends Message
    {
        protected UUID tracingId;
        protected List<String> warnings;

        protected Response(Type type)
        {
            super(type);

            if (type.direction != Direction.RESPONSE)
                throw new IllegalArgumentException();
        }

        public Message setTracingId(UUID tracingId)
        {
            this.tracingId = tracingId;
            return this;
        }

        public UUID getTracingId()
        {
            return tracingId;
        }

        public Message setWarnings(List<String> warnings)
        {
            this.warnings = warnings;
            return this;
        }

        public List<String> getWarnings()
        {
            return warnings;
        }
    }

    public Envelope encode(ProtocolVersion version)
    {
        EnumSet<Envelope.Header.Flag> flags = EnumSet.noneOf(Envelope.Header.Flag.class);
        @SuppressWarnings("unchecked")
        Codec<Message> codec = (Codec<Message>)this.type.codec;
        try
        {
            int messageSize = codec.encodedSize(this, version);
            ByteBuf body;
            if (this instanceof Response)
            {
                Response message = (Response)this;
                UUID tracingId = message.getTracingId();
                Map<String, ByteBuffer> customPayload = message.getCustomPayload();
                if (tracingId != null)
                    messageSize += CBUtil.sizeOfUUID(tracingId);
                List<String> warnings = message.getWarnings();
                if (warnings != null)
                {
                    if (version.isSmallerThan(ProtocolVersion.V4))
                        throw new ProtocolException("Must not send frame with WARNING flag for native protocol version < 4");
                    messageSize += CBUtil.sizeOfStringList(warnings);
                }
                if (customPayload != null)
                {
                    if (version.isSmallerThan(ProtocolVersion.V4))
                        throw new ProtocolException("Must not send frame with CUSTOM_PAYLOAD flag for native protocol version < 4");
                    messageSize += CBUtil.sizeOfBytesMap(customPayload);
                }
                body = CBUtil.allocator.buffer(messageSize);
                if (tracingId != null)
                {
                    CBUtil.writeUUID(tracingId, body);
                    flags.add(Envelope.Header.Flag.TRACING);
                }
                if (warnings != null)
                {
                    CBUtil.writeStringList(warnings, body);
                    flags.add(Envelope.Header.Flag.WARNING);
                }
                if (customPayload != null)
                {
                    CBUtil.writeBytesMap(customPayload, body);
                    flags.add(Envelope.Header.Flag.CUSTOM_PAYLOAD);
                }
            }
            else
            {
                assert this instanceof Request;
                if (((Request)this).isTracingRequested())
                    flags.add(Envelope.Header.Flag.TRACING);
                Map<String, ByteBuffer> payload = getCustomPayload();
                if (payload != null)
                    messageSize += CBUtil.sizeOfBytesMap(payload);
                body = CBUtil.allocator.buffer(messageSize);
                if (payload != null)
                {
                    CBUtil.writeBytesMap(payload, body);
                    flags.add(Envelope.Header.Flag.CUSTOM_PAYLOAD);
                }
            }

            try
            {
                codec.encode(this, body, version);
            }
            catch (Throwable e)
            {
                body.release();
                throw e;
            }

            // if the driver attempted to connect with a protocol version lower than the minimum supported
            // version, respond with a protocol error message with the correct message header for that version
            ProtocolVersion responseVersion = forcedProtocolVersion == null
                                              ? version
                                              : forcedProtocolVersion;

            if (responseVersion.isBeta())
                flags.add(Envelope.Header.Flag.USE_BETA);

            return Envelope.create(type, getStreamId(), responseVersion, flags, body);
        }
        catch (Throwable e)
        {
            throw ErrorMessage.wrap(e, getStreamId());
        }
    }

    abstract static class Decoder<M extends Message>
    {
        static Message decodeMessage(Channel channel, Envelope inbound)
        {
            boolean isRequest = inbound.header.type.direction == Direction.REQUEST;
            boolean isTracing = inbound.header.flags.contains(Envelope.Header.Flag.TRACING);
            boolean isCustomPayload = inbound.header.flags.contains(Envelope.Header.Flag.CUSTOM_PAYLOAD);
            boolean hasWarning = inbound.header.flags.contains(Envelope.Header.Flag.WARNING);

            UUID tracingId = isRequest || !isTracing ? null : CBUtil.readUUID(inbound.body);
            List<String> warnings = isRequest || !hasWarning ? null : CBUtil.readStringList(inbound.body);
            Map<String, ByteBuffer> customPayload = !isCustomPayload ? null : CBUtil.readBytesMap(inbound.body);

            if (isCustomPayload && inbound.header.version.isSmallerThan(ProtocolVersion.V4))
                throw new ProtocolException("Received frame with CUSTOM_PAYLOAD flag for native protocol version < 4");

            Message message = inbound.header.type.codec.decode(inbound.body, inbound.header.version);
            message.setStreamId(inbound.header.streamId);
            message.setSource(inbound);
            message.setCustomPayload(customPayload);

            if (isRequest)
            {
                assert message instanceof Request;
                Request req = (Request) message;
                Connection connection = channel.attr(Connection.attributeKey).get();
                req.attach(connection);
                if (isTracing)
                    req.setTracingRequested();
            }
            else
            {
                assert message instanceof Response;
                if (isTracing)
                    ((Response) message).setTracingId(tracingId);
                if (hasWarning)
                    ((Response) message).setWarnings(warnings);
            }
            return message;
        }

        abstract M decode(Channel channel, Envelope inbound);

        private static class RequestDecoder extends Decoder<Request>
        {
            Request decode(Channel channel, Envelope request)
            {
                if (request.header.type.direction != Direction.REQUEST)
                    throw new ProtocolException(String.format("Unexpected RESPONSE message %s, expecting REQUEST",
                                                              request.header.type));

                return (Request) decodeMessage(channel, request);
            }
        }

        private static class ResponseDecoder extends Decoder<Response>
        {
            Response decode(Channel channel, Envelope response)
            {
                if (response.header.type.direction != Direction.RESPONSE)
                    throw new ProtocolException(String.format("Unexpected REQUEST message %s, expecting RESPONSE",
                                                              response.header.type));

                return (Response) decodeMessage(channel, response);
            }
        }
    }

    private static final Decoder.RequestDecoder REQUEST_DECODER = new Decoder.RequestDecoder();
    private static final Decoder.ResponseDecoder RESPONSE_DECODER = new Decoder.ResponseDecoder();

    static Decoder<Message.Request> requestDecoder()
    {
        return REQUEST_DECODER;
    }

    static Decoder<Message.Response> responseDecoder()
    {
        return RESPONSE_DECODER;
    }
}
