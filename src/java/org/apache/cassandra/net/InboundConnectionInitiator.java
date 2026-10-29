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
package org.apache.cassandra.net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.cert.Certificate;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.cassandra.auth.IInternodeAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.OutboundConnectionSettings.Framing;
import org.apache.cassandra.security.ISslContextFactory;
import org.apache.cassandra.security.SSLFactory;
import org.apache.cassandra.streaming.StreamDeserializingTask;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.streaming.async.NettyStreamingChannel;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.memory.BufferPools;

import static java.lang.Math.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.INBOUND;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.net.InternodeConnectionUtils.DISCARD_HANDLER_NAME;
import static org.apache.cassandra.net.InternodeConnectionUtils.SSL_FACTORY_CONTEXT_DESCRIPTION;
import static org.apache.cassandra.net.InternodeConnectionUtils.SSL_HANDLER_NAME;
import static org.apache.cassandra.net.InternodeConnectionUtils.certificates;
import static org.apache.cassandra.net.MessagingService.*;
import static org.apache.cassandra.net.SocketFactory.WIRETRACE;
import static org.apache.cassandra.net.SocketFactory.newSslHandler;

public class InboundConnectionInitiator
{
    private static final Logger logger = LoggerFactory.getLogger(InboundConnectionInitiator.class);

    private static final NoSpamLogger noSpam5m = NoSpamLogger.getLogger(logger, 5, TimeUnit.MINUTES);

    private static class Initializer extends ChannelInitializer<SocketChannel>
    {
        private static final String PIPELINE_INTERNODE_ERROR_EXCLUSIONS = "Internode Error Exclusions";

        private final InboundConnectionSettings settings;
        private final ChannelGroup channelGroup;
        private final Consumer<ChannelPipeline> pipelineInjector;

        Initializer(InboundConnectionSettings settings, ChannelGroup channelGroup,
                    Consumer<ChannelPipeline> pipelineInjector)
        {
            this.settings = settings;
            this.channelGroup = channelGroup;
            this.pipelineInjector = pipelineInjector;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception
        {
            // if any of the handlers added fail they will send the error to the "head", so this needs to be first
            channel.pipeline().addFirst(PIPELINE_INTERNODE_ERROR_EXCLUSIONS, new InternodeErrorExclusionsHandler());

            channelGroup.add(channel);

            channel.config().setOption(ChannelOption.ALLOCATOR, GlobalBufferPoolAllocator.instance);
            channel.config().setOption(ChannelOption.SO_KEEPALIVE, true);
            channel.config().setOption(ChannelOption.SO_REUSEADDR, true);
            channel.config().setOption(ChannelOption.TCP_NODELAY, true); // we only send handshake messages; no point ever delaying

            ChannelPipeline pipeline = channel.pipeline();

            pipelineInjector.accept(pipeline);

            // order of handlers: ssl -> client-authentication -> logger -> handshakeHandler
            // For either unencrypted or transitional modes, allow Ssl optionally.
            switch(settings.encryption.tlsEncryptionPolicy())
            {
                case UNENCRYPTED:
                    // Handler checks for SSL connection attempts and cleanly rejects them if encryption is disabled
                    pipeline.addAfter(PIPELINE_INTERNODE_ERROR_EXCLUSIONS, "rejectssl", new RejectSslHandler());
                    break;
                case OPTIONAL:
                    pipeline.addAfter(PIPELINE_INTERNODE_ERROR_EXCLUSIONS, SSL_HANDLER_NAME, new OptionalSslHandler(settings.encryption));
                    break;
                case ENCRYPTED:
                    SslHandler sslHandler = getSslHandler("creating", channel, settings.encryption);
                    pipeline.addAfter(PIPELINE_INTERNODE_ERROR_EXCLUSIONS, SSL_HANDLER_NAME, sslHandler);
                    break;
            }

            // Pipeline for performing client authentication
            pipeline.addLast("client-authentication", new ClientAuthenticationHandler(settings.authenticator));

            if (WIRETRACE)
                pipeline.addLast("logger", new LoggingHandler(LogLevel.INFO));

            channel.pipeline().addLast("handshake", new Handler(settings));
        }
    }

    private static class InternodeErrorExclusionsHandler extends ChannelInboundHandlerAdapter
    {
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
        {
            if (DatabaseDescriptor.getInternodeErrorReportingExclusions().contains(ctx.channel().remoteAddress()))
            {
                logger.debug("Excluding internode exception for {}; address contained in internode_error_reporting_exclusions", ctx.channel().remoteAddress(), cause);
                return;
            }
            super.exceptionCaught(ctx, cause);
        }
    }

    /**
     * Create a {@link Channel} that listens on the {@code localAddr}. This method will block while trying to bind to the address,
     * but it does not make a remote call.
     */
    private static ChannelFuture bind(Initializer initializer) throws ConfigurationException
    {
        logger.info("Listening on {}", initializer.settings);

        ServerBootstrap bootstrap = initializer.settings.socketFactory
                                    .newServerBootstrap()
                                    .option(ChannelOption.SO_BACKLOG, 1 << 9)
                                    .option(ChannelOption.ALLOCATOR, GlobalBufferPoolAllocator.instance)
                                    .option(ChannelOption.SO_REUSEADDR, true)
                                    .childHandler(initializer);

        int socketReceiveBufferSizeInBytes = initializer.settings.socketReceiveBufferSizeInBytes;
        if (socketReceiveBufferSizeInBytes > 0)
            bootstrap.childOption(ChannelOption.SO_RCVBUF, socketReceiveBufferSizeInBytes);

        InetAddressAndPort bind = initializer.settings.bindAddress;
        ChannelFuture channelFuture = bootstrap.bind(new InetSocketAddress(bind.getAddress(), bind.getPort()));

        if (!channelFuture.awaitUninterruptibly().isSuccess())
        {
            if (channelFuture.channel().isOpen())
                channelFuture.channel().close();

            Throwable failedChannelCause = channelFuture.cause();

            String causeString = "";
            if (failedChannelCause != null && failedChannelCause.getMessage() != null)
                causeString = failedChannelCause.getMessage();

            if (causeString.contains("in use"))
            {
                throw new ConfigurationException(bind + " is in use by another process.  Change listen_address:storage_port " +
                                                 "in cassandra.yaml to values that do not conflict with other services");
            }
            else if (causeString.contains("cannot assign requested address"))
            {
                throw new ConfigurationException("Unable to bind to address " + bind
                                                 + ". Set listen_address in cassandra.yaml to an interface you can bind to, e.g., your private IP address on EC2");
            }
            else
            {
                throw new ConfigurationException("failed to bind to: " + bind, failedChannelCause);
            }
        }

        return channelFuture;
    }

    public static ChannelFuture bind(InboundConnectionSettings settings, ChannelGroup channelGroup,
                                     Consumer<ChannelPipeline> pipelineInjector)
    {
        return bind(new Initializer(settings, channelGroup, pipelineInjector));
    }

    /**
     * Handler to perform authentication for internode inbound connections.
     * This handler is called even before messaging handshake starts.
     */
    private static class ClientAuthenticationHandler extends ByteToMessageDecoder
    {
        private final IInternodeAuthenticator authenticator;

        public ClientAuthenticationHandler(IInternodeAuthenticator authenticator)
        {
            this.authenticator = authenticator;
        }

        @Override
        protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf byteBuf, List<Object> list) throws Exception
        {
            // Extract certificates from SSL handler(handler with name "ssl").
            final Certificate[] certificates = certificates(channelHandlerContext.channel());
            if (!authenticate(channelHandlerContext.channel().remoteAddress(), certificates))
            {
                logger.error("Unable to authenticate peer {} for internode authentication", channelHandlerContext.channel());

                // To release all the pending buffered data, replace authentication handler with discard handler.
                // This avoids pending inbound data to be fired through the pipeline
                channelHandlerContext.pipeline().replace(this, DISCARD_HANDLER_NAME, new InternodeConnectionUtils.ByteBufDiscardHandler());
                channelHandlerContext.pipeline().close();
            }
            else
            {
                channelHandlerContext.pipeline().remove(this);
            }
        }

        private boolean authenticate(SocketAddress socketAddress, final Certificate[] certificates) throws IOException
        {
            if (socketAddress.getClass().getSimpleName().equals("EmbeddedSocketAddress"))
                return true;

            if (!(socketAddress instanceof InetSocketAddress))
                throw new IOException(String.format("Unexpected SocketAddress type: %s, %s", socketAddress.getClass(), socketAddress));

            InetSocketAddress addr = (InetSocketAddress) socketAddress;
            if (!authenticator.authenticate(addr.getAddress(), addr.getPort(), certificates, INBOUND))
            {
                // Log at info level as anything that can reach the inbound port could hit this
                // and trigger a log of noise.  Failed outbound connections to known cluster endpoints
                // still fail with an ERROR message and exception to alert operators that aren't watching logs closely.
                logger.info("Authenticate rejected inbound internode connection from {}", addr);
                return false;
            }
            return true;
        }

    }

    /**
     * 'Server-side' component that negotiates the internode handshake when establishing a new connection.
     * This handler will be the first in the netty channel for each incoming connection (secure socket (TLS) notwithstanding),
     * and once the handshake is successful, it will configure the proper handlers ({@link InboundMessageHandler}
     * or {@link StreamingInboundHandler}) and remove itself from the working pipeline.
     */
    static class Handler extends ByteToMessageDecoder
    {
        private final InboundConnectionSettings settings;

        private HandshakeProtocol.Initiate initiate;
        /** The third message of a pre-4.0 handshake (DSE 6 compatibility mode), once received. */
        private HandshakeProtocol.ConfirmOutboundPre40 confirmOutboundPre40;
        /** True once our 4-byte answer (the dialect) to a pre-4.0 first message was queued (DSE 6 compatibility mode). */
        private boolean pre40Answered;
        /** True once any byte of a third pre-4.0 handshake message reached this handler, even if the socket closed since. */
        private boolean pre40ConfirmStarted;

        /**
         * A future the essentially places a timeout on how long we'll wait for the peer
         * to complete the next step of the handshake.
         */
        private Future<?> handshakeTimeout;

        Handler(InboundConnectionSettings settings)
        {
            this.settings = settings;
        }

        /**
         * On registration, immediately schedule a timeout to kill this connection if it does not handshake promptly.
         */
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception
        {
            handshakeTimeout = ctx.executor().schedule(() -> {
                InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
                if (isAbandonedByDse())
                {
                    logger.debug("peer {} did not send the third message of the pre-4.0 handshake (requested version {}, answered {}) " +
                                 "within {} ms of the connection (on {}): expected from a DSE 6.8.65 node, which gives up {} " +
                                 "and reconnects at {} on a new one; closing the socket",
                                 remote, initiate.requestMessagingVersion, DseCompat.DSE_DIALECT, HandshakeProtocol.TIMEOUT_MILLIS,
                                 settings.bindAddress, abandonedSocketDescription(), DseCompat.DSE_DIALECT);
                    failHandshake(ctx);
                    return;
                }
                logger.error("Timeout handshaking with {} (on {})",
                             initiate != null && initiate.from != null ? SocketFactory.addressId(initiate.from, remote) : remote,
                             settings.bindAddress);
                failHandshake(ctx);
            }, HandshakeProtocol.TIMEOUT_MILLIS, MILLISECONDS);
        }

        /**
         * True when the handshake is stuck on a socket a DSE 6.8.65 node gave up on purpose (DSE 6 compatibility mode
         * only): its pre-4.0 first message requested either 12 (a plain first message at
         * {@link DseCompat#NET_VERSION_PUBLISHED}) or {@link DseCompat#DSE_VERSION_BYTE} (DSE's own flagged first
         * message), we answered the dialect (11) and no byte reached this handler since (on a TLS connection, no
         * decrypted byte). In both cases DSE 6.8.65 gives this socket up without a third message and reconnects at 11 on a
         * new socket, which succeeds, as observed with DSE 6.8.65: for a plain first message at 12, DSE logs "Seed gossip
         * version is 11; will not connect with that version" as a WARN when this node is one of its seeds; for its flagged
         * first message, DSE closes the socket it gave up at once (it never shows in {@code ss -tn} on our node),
         * reconnects at 11 on a new one about 120 ms later and logs the reconnection. DSE 6.9.26 does not give such a
         * socket up: it sends the third message on the same socket after either first message (every socket an upgraded
         * node accepted from a DSE 6.9.26 node reached an established connection), so this case is not expected with
         * it. A socket closed before the timeout is handled by {@link #channelInactive}; a socket left
         * open reaches the handshake timeout, which closes it.
         * <p>
         * A request at 10 or 11 does not qualify (no DSE node requests 10 from us, and after a request at 11 DSE sends the
         * third message on the same socket), nor does a third message of which some bytes arrived, even if the peer closed
         * the socket since: those keep the upstream error.
         */
        private boolean isAbandonedByDse()
        {
            return DseCompat.enabled()
                   && pre40Answered
                   && confirmOutboundPre40 == null
                   && (initiate.requestMessagingVersion == DseCompat.NET_VERSION_PUBLISHED
                       || initiate.requestMessagingVersion == DseCompat.DSE_VERSION_BYTE)
                   && !pre40ConfirmStarted;
        }

        /** What a DSE 6.8.65 node gives up in the case {@link #isAbandonedByDse} recognises, for the debug lines. */
        private String abandonedSocketDescription()
        {
            return initiate.requestMessagingVersion == DseCompat.DSE_VERSION_BYTE
                   ? "the socket of its own flagged first message once answered with a plain pre-4.0 version"
                   : "a socket answered below the version it stored for us";
        }

        /**
         * The upstream handler does not cancel the handshake timeout when the channel closes, so a socket the peer closed
         * before the handshake completed still logs {@code Timeout handshaking with ...} when the timeout fires, and this
         * stays so for every case but the one {@link #isAbandonedByDse} recognises (DSE 6 compatibility mode only): such a
         * socket that closes cancels the timeout with a debug line, since a DSE 6.8.65 node closes it to reconnect at 11.
         * <p>
         * Our handshake failures ({@link #failHandshake}, also run by the timeout) remove this handler before closing and
         * a completed handshake removes it, so neither gets here; the close of every accepted socket when the inbound
         * sockets shut down does, which is why the debug line does not say who closed the connection. On a TLS connection
         * {@code SslHandler} passes the close on to this handler; that case is reasoned from netty's code, not covered by
         * a test.
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception
        {
            super.channelInactive(ctx);
            if (isAbandonedByDse() && handshakeTimeout != null && handshakeTimeout.cancel(true))
            {
                logger.debug("the connection from peer {} (on {}) closed without the third message of the pre-4.0 handshake " +
                             "(requested version {}, answered {}): expected from a DSE 6.8.65 node, which gives up {} " +
                             "and reconnects at {} on a new one; handshake timeout cancelled",
                             ctx.channel().remoteAddress(), settings.bindAddress, initiate.requestMessagingVersion, DseCompat.DSE_DIALECT,
                             abandonedSocketDescription(), DseCompat.DSE_DIALECT);
            }
        }

        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
        {
            if (initiate == null)
                initiate(ctx, in);
            else if (initiate.isPre40() && confirmOutboundPre40 == null)
            {
                if (in.isReadable())
                    pre40ConfirmStarted = true;
                confirmPre40(ctx, in);
            }
            else
                throw new IllegalStateException("Should no longer be on pipeline");
        }

        void initiate(ChannelHandlerContext ctx, ByteBuf in) throws IOException
        {
            initiate = HandshakeProtocol.Initiate.maybeDecode(in);
            if (initiate == null)
                return;

            logger.trace("Received handshake initiation message from peer {}, message = {}", ctx.channel().remoteAddress(), initiate);

            if (initiate.isPre40())
            {
                initiatePre40(ctx);
                return;
            }

            if (isEncryptionRequired(initiate.from) && !isChannelEncrypted(ctx))
            {
                logger.warn("peer {} attempted to establish an unencrypted connection (broadcast address {})",
                            ctx.channel().remoteAddress(), initiate.from);
                failHandshake(ctx);
                return;
            }

            assert initiate.acceptVersions != null;
            logger.trace("Connection version {} (min {}) from {}", initiate.acceptVersions.max, initiate.acceptVersions.min, initiate.from);

            final AcceptVersions accept;

            if (initiate.type.isStreaming())
                accept = settings.acceptStreaming;
            else
                accept = settings.acceptMessaging;

            int useMessagingVersion = max(accept.min, min(accept.max, initiate.acceptVersions.max));
            ByteBuf flush = new HandshakeProtocol.Accept(useMessagingVersion, accept.max).encode(ctx.alloc());

            AsyncChannelPromise.writeAndFlush(ctx, flush, (ChannelFutureListener) future -> {
                if (!future.isSuccess())
                    exceptionCaught(future.channel(), future.cause());
            });

            if (initiate.acceptVersions.min > accept.max)
            {
                logger.info("peer {} only supports messaging versions higher ({}) than this node supports ({})", ctx.channel().remoteAddress(), initiate.acceptVersions.min, current_version);
                failHandshake(ctx);
            }
            else if (initiate.acceptVersions.max < accept.min)
            {
                logger.info("peer {} only supports messaging versions lower ({}) than this node supports ({})", ctx.channel().remoteAddress(), initiate.acceptVersions.max, minimum_version);
                failHandshake(ctx);
            }
            else
            {
                if (initiate.type.isStreaming())
                    setupStreamingPipeline(initiate.from, useMessagingVersion, ctx);
                else
                    setupMessagingPipeline(initiate.from, useMessagingVersion, initiate.acceptVersions.max, ctx.pipeline());
            }
        }

        /**
         * A pre-4.0 first message, only decoded in DSE 6 compatibility mode: DSE's own first message (0xFF plus its
         * relative version) or a plain pre-4.0 one. We answer with the 3.0.14 dialect at once, whatever was
         * requested, and wait for the third message. DSE 6.9.26 sends it on the same socket, after its own first message
         * and after a plain first message at 12, as DSE's source has it; DSE 6.8.65 gives the socket up after either and
         * reconnects at 11 on a new one instead (see {@link #isAbandonedByDse}).
         * <p>
         * Everything that can refuse the connection before the third message is checked before we answer, so a refused
         * peer gets no second message: streaming, an obsolete version (below 3.0, which DSE also closes without an
         * answer) and the encryption requirement, evaluated here on the socket's remote IP address with the
         * default storage port (see {@link #pre40EncryptionCheckAddress}). The peer's broadcast address is only known
         * from the third message, where the encryption requirement is checked again.
         */
        private void initiatePre40(ChannelHandlerContext ctx)
        {
            int requested = initiate.requestMessagingVersion;
            InetSocketAddress remote = (InetSocketAddress) ctx.channel().remoteAddress();
            // each refusal is logged at every attempt, as the refusals of the 4.0-style handshake are: a misconfigured
            // peer stays visible while it retries, at the pace of its reconnection delay
            if (initiate.type.isStreaming())
            {
                logger.warn("peer {} attempted a pre-4.0 streaming connection (requested version {}): streaming with DSE 6 peers is not supported in internode_compatibility_mode DSE_6",
                            remote, requested);
                failHandshake(ctx);
                return;
            }

            if (requested != DseCompat.DSE_VERSION_BYTE && requested < DseCompat.OLDEST_PRE40_VERSION)
            {
                logger.warn("peer {} requested the obsolete messaging version {}; the earliest version supported is 3.0.0 (10), closing",
                            remote, requested);
                failHandshake(ctx);
                return;
            }

            if (isEncryptionRequired(pre40EncryptionCheckAddress(remote)) && !isChannelEncrypted(ctx))
            {
                logger.warn("peer {} attempted to establish an unencrypted pre-4.0 connection", remote);
                failHandshake(ctx);
                return;
            }

            logger.debug("peer {} sent a pre-4.0 handshake (requested version {}); answering {}", remote, requested, DseCompat.DSE_DIALECT);
            ByteBuf response = HandshakeProtocol.Accept.respondPre40(DseCompat.DSE_DIALECT, ctx.alloc());
            AsyncChannelPromise.writeAndFlush(ctx, response, (ChannelFutureListener) future -> {
                if (!future.isSuccess())
                    exceptionCaught(future.channel(), future.cause());
            });
            pre40Answered = true;

            // the pipeline is set up once the third message arrives (confirmPre40)
        }

        /**
         * The address the encryption requirement of a pre-4.0 first message is evaluated on: the socket's source port is
         * ephemeral, so the peer is looked up by IP address with the default storage port, as the snitch knows it
         * ({@code internode_encryption: dc} or {@code rack}).
         */
        @VisibleForTesting
        static InetAddressAndPort pre40EncryptionCheckAddress(InetSocketAddress remote)
        {
            return InetAddressAndPort.getByAddress(remote.getAddress());
        }

        /**
         * The third (and last) message of the pre-4.0 handshake: the peer's version and address. Only the dialect we
         * answered with is accepted.
         */
        @VisibleForTesting
        void confirmPre40(ChannelHandlerContext ctx, ByteBuf in)
        {
            try
            {
                confirmOutboundPre40 = HandshakeProtocol.ConfirmOutboundPre40.maybeDecode(in);
            }
            catch (HandshakeProtocol.InvalidPre40Address e)
            {
                logger.warn("peer {} sent a third message of the pre-4.0 handshake with an address of {} bytes, neither IPv4 nor IPv6; closing",
                            ctx.channel().remoteAddress(), e.size);
                failHandshake(ctx);
                return;
            }
            if (confirmOutboundPre40 == null)
                return;

            logger.trace("Received third handshake message from peer {}, message = {}", ctx.channel().remoteAddress(), confirmOutboundPre40);
            if (confirmOutboundPre40.maxMessagingVersion != DseCompat.DSE_DIALECT)
            {
                logger.warn("peer {} ({}) confirmed the pre-4.0 handshake with version {} instead of the {} we answered; closing",
                            ctx.channel().remoteAddress(), confirmOutboundPre40.from, confirmOutboundPre40.maxMessagingVersion, DseCompat.DSE_DIALECT);
                failHandshake(ctx);
                return;
            }
            if (isEncryptionRequired(confirmOutboundPre40.from) && !isChannelEncrypted(ctx))
            {
                logger.warn("peer {} attempted to establish an unencrypted connection (broadcast address {})",
                            ctx.channel().remoteAddress(), confirmOutboundPre40.from);
                failHandshake(ctx);
                return;
            }
            // the first message may have been one only DSE sends, which identifies the peer the third message names as a
            // DSE node (DsePeers): DSE 6.9.26 sends this third message after either such first message (every connection
            // of a DSE 6.9.26 node to an upgraded node was seen to open with the flagged one); DSE 6.8.65 sends none after
            // them (it reconnects at 11, see isAbandonedByDse), so with it this only sees plain requests at 10 or 11
            DsePeers.fromPre40Initiate(confirmOutboundPre40.from, initiate.requestMessagingVersion);
            setupMessagingPipeline(confirmOutboundPre40.from, DseCompat.DSE_DIALECT, DseCompat.DSE_DIALECT, ctx.pipeline());
        }

        private boolean isEncryptionRequired(InetAddressAndPort peer)
        {
            return !settings.encryption.isExplicitlyOptional() && settings.encryption.shouldEncrypt(peer);
        }

        private boolean isChannelEncrypted(ChannelHandlerContext ctx)
        {
            return ctx.pipeline().get(SslHandler.class) != null;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
        {
            exceptionCaught(ctx.channel(), cause);
        }

        private void exceptionCaught(Channel channel, Throwable cause)
        {
            final SocketAddress remoteAddress = channel.remoteAddress();
            boolean reportingExclusion = DatabaseDescriptor.getInternodeErrorReportingExclusions().contains(remoteAddress);

            if (reportingExclusion)
                logger.debug("Excluding internode exception for {}; address contained in internode_error_reporting_exclusions", remoteAddress, cause);
            else if (cause != null && Throwables.getRootCause(cause) instanceof Message.InvalidLegacyProtocolMagic && DatabaseDescriptor.getInvalidLegacyProtocolMagicNoSpamEnabled())
                noSpam5m.warn("Failed to properly handshake with peer {}. Closing the channel. Invalid legacy protocol magic.", ((InetSocketAddress) channel.remoteAddress()).getHostName());
            else
                logger.error("Failed to properly handshake with peer {}. Closing the channel.", remoteAddress, cause);

            try
            {
                failHandshake(channel);
            }
            catch (Throwable t)
            {
                if (!reportingExclusion)
                    logger.error("Unexpected exception in {}.exceptionCaught", this.getClass().getSimpleName(), t);
            }
        }

        private void failHandshake(ChannelHandlerContext ctx)
        {
            failHandshake(ctx.channel());
        }

        private void failHandshake(Channel channel)
        {
            // Cancel the handshake timeout as early as possible as it calls this method
            if (handshakeTimeout != null)
                handshakeTimeout.cancel(true);

            // prevent further decoding of buffered data by removing this handler before closing
            // otherwise the pending bytes will be decoded again on close, throwing further exceptions.
            try
            {
                channel.pipeline().remove(this);
            }
            catch (NoSuchElementException ex)
            {
                // possible race with the handshake timeout firing and removing this handler already
            }
            finally
            {
                channel.close();
            }
        }

        private void setupStreamingPipeline(InetAddressAndPort from, int streamingVersion, ChannelHandlerContext ctx)
        {
            handshakeTimeout.cancel(true);
            assert initiate.framing == Framing.UNPROTECTED;

            ChannelPipeline pipeline = ctx.pipeline();
            Channel channel = ctx.channel();

            if (from == null)
            {
                InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
                from = InetAddressAndPort.getByAddressOverrideDefaults(address.getAddress(), address.getPort());
            }

            BufferPools.forNetworking().setRecycleWhenFreeForCurrentThread(false);

            // we can't infer the type of streaming connection at this point,
            // so we use CONTROL unconditionally; it's ugly but does what we want
            // (establishes an AsyncStreamingInputPlus)
            channel.attr(NettyStreamingChannel.STREAMING_VERSION_ATTR).set(streamingVersion);
            NettyStreamingChannel streamingChannel = new NettyStreamingChannel(channel, StreamingChannel.Kind.CONTROL);
            pipeline.replace(this, "streamInbound", streamingChannel);
            executorFactory().startThread(String.format("Stream-Deserializer-%s-%s", from, channel.id()),
                                          new StreamDeserializingTask(null, streamingChannel, streamingVersion));

            logger.info("{} streaming connection established, version = {}, framing = {}, encryption = {}",
                        SocketFactory.channelId(from,
                                                (InetSocketAddress) channel.remoteAddress(),
                                                settings.bindAddress,
                                                (InetSocketAddress) channel.localAddress(),
                                                ConnectionType.STREAMING,
                                                channel.id().asShortText()),
                        streamingVersion,
                        initiate.framing,
                        SocketFactory.encryptionConnectionSummary(pipeline.channel()));
        }

        @VisibleForTesting
        void setupMessagingPipeline(InetAddressAndPort from, int useMessagingVersion, int maxMessagingVersion, ChannelPipeline pipeline)
        {
            handshakeTimeout.cancel(true);
            // record the "true" endpoint, i.e. the one the peer is identified with, as opposed to the socket it connected over
            if (DseCompat.enabled())
                // not lowered to the dialect for a peer gossiping 12..255, unless only DSE sends its first message
                DseDialectVersions.recordHandshakeVersion(from, maxMessagingVersion, initiate.requestMessagingVersion);
            else
                instance().versions.set(from, maxMessagingVersion);

            BufferPools.forNetworking().setRecycleWhenFreeForCurrentThread(false);
            BufferPoolAllocator allocator = GlobalBufferPoolAllocator.instance;
            if (initiate.type == ConnectionType.LARGE_MESSAGES)
            {
                // for large messages, swap the global pool allocator for a local one, to optimise utilisation of chunks
                allocator = new LocalBufferPoolAllocator(pipeline.channel().eventLoop());
                pipeline.channel().config().setAllocator(allocator);
            }

            FrameDecoder frameDecoder;
            boolean pre40 = useMessagingVersion < VERSION_40; // DSE 6 compatibility mode: unframed, or the pre-4.0 LZ4 block stream
            switch (initiate.framing)
            {
                case LZ4:
                {
                    frameDecoder = pre40 ? new FrameDecoderLegacyLZ4(allocator, useMessagingVersion) : FrameDecoderLZ4.fast(allocator);
                    break;
                }
                case CRC:
                {
                    // pre-4.0 peers never CRC their frames
                    frameDecoder = pre40 ? new FrameDecoderLegacy(allocator, useMessagingVersion) : FrameDecoderCrc.create(allocator);
                    break;
                }
                case UNPROTECTED:
                {
                    frameDecoder = pre40 ? new FrameDecoderLegacy(allocator, useMessagingVersion) : new FrameDecoderUnprotected(allocator);
                    break;
                }
                default:
                    throw new AssertionError();
            }

            frameDecoder.addLastTo(pipeline);

            InboundMessageHandler handler =
                settings.handlers.apply(from).createHandler(frameDecoder, initiate.type, pipeline.channel(), useMessagingVersion);

            logger.info("{} messaging connection established, version = {}, framing = {}, encryption = {}",
                        handler.id(true),
                        useMessagingVersion,
                        initiate.framing,
                        SocketFactory.encryptionConnectionSummary(pipeline.channel()));

            pipeline.addLast("deserialize", handler);

            try
            {
                pipeline.remove(this);
            }
            catch (NoSuchElementException ex)
            {
                // possible race with the handshake timeout firing and removing this handler already
            }
        }
    }

    private static SslHandler getSslHandler(String description, Channel channel, EncryptionOptions.ServerEncryptionOptions encryptionOptions) throws IOException
    {
        final boolean verifyPeerCertificate = true;
        SslContext sslContext = SSLFactory.getOrCreateSslContext(encryptionOptions, verifyPeerCertificate,
                                                                 ISslContextFactory.SocketType.SERVER,
                                                                 SSL_FACTORY_CONTEXT_DESCRIPTION);
        InetSocketAddress peer = encryptionOptions.require_endpoint_verification ? (InetSocketAddress) channel.remoteAddress() : null;
        SslHandler sslHandler = newSslHandler(channel, sslContext, peer);
        logger.trace("{} inbound netty SslContext: context={}, engine={}", description, sslContext.getClass().getName(), sslHandler.engine().getClass().getName());
        return sslHandler;
    }

    private static class OptionalSslHandler extends ByteToMessageDecoder
    {
        private final EncryptionOptions.ServerEncryptionOptions encryptionOptions;

        OptionalSslHandler(EncryptionOptions.ServerEncryptionOptions encryptionOptions)
        {
            this.encryptionOptions = encryptionOptions;
        }

        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception
        {
            if (in.readableBytes() < 5)
            {
                // To detect if SSL must be used we need to have at least 5 bytes, so return here and try again
                // once more bytes a ready.
                return;
            }

            if (SslHandler.isEncrypted(in))
            {
                // Connection uses SSL/TLS, replace the detection handler with a SslHandler and so use encryption.
                SslHandler sslHandler = getSslHandler("replacing optional", ctx.channel(), encryptionOptions);
                ctx.pipeline().replace(this, SSL_HANDLER_NAME, sslHandler);
            }
            else
            {
                // Connection use no TLS/SSL encryption, just remove the detection handler and continue without
                // SslHandler in the pipeline.
                ctx.pipeline().remove(this);
            }
        }
    }

    private static class RejectSslHandler extends ByteToMessageDecoder
    {
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out)
        {
            if (in.readableBytes() < 5)
            {
                // To detect if SSL must be used we need to have at least 5 bytes, so return here and try again
                // once more bytes a ready.
                return;
            }

            if (SslHandler.isEncrypted(in))
            {
                logger.info("Rejected incoming TLS connection before negotiating from {} to {}. TLS is explicitly disabled by configuration.",
                            ctx.channel().remoteAddress(), ctx.channel().localAddress());
                in.readBytes(in.readableBytes()); // discard the readable bytes so not called again
                ctx.close();
            }
            else
            {
                // Incoming connection did not attempt TLS/SSL encryption, just remove the detection handler and continue without
                // SslHandler in the pipeline.
                ctx.pipeline().remove(this);
            }
        }
    }
}
