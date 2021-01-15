/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection.netty;

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.Stream;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.ReadTimeoutException;
import org.bson.ByteBuf;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.SocketAddress;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.connection.SslHelper.enableHostNameVerification;
import static com.mongodb.internal.connection.SslHelper.enableSni;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A Stream implementation based on Netty 4.0.
 * Just like it is for the {@link java.nio.channels.AsynchronousSocketChannel},
 * concurrent pending<sup>1</sup> readers
 * (whether {@linkplain #read(int, int) synchronous} or {@linkplain #readAsync(int, AsyncCompletionHandler, int) asynchronous})
 * are not supported by {@link NettyStream}.
 * However, this class does not have a fail-fast mechanism checking for such situations.
 * <hr>
 * <sup>1</sup>We cannot simply say that read methods are not allowed be run concurrently because strictly speaking they are allowed,
 * as explained below.
 * <pre>{@code
 * NettyStream stream = ...;
 * stream.readAsync(1, new AsyncCompletionHandler<ByteBuf>() {//inv1
 *  @Override
 *  public void completed(ByteBuf o) {
 *      stream.readAsync(//inv2
 *              1, ...);//ret2
 *  }
 *
 *  @Override
 *  public void failed(Throwable t) {
 *  }
 * });//ret1
 * }</pre>
 * Arrows on the diagram below represent happens-before relations.
 * <pre>{@code
 * int1 -> inv2 -> ret2
 *      \--------> ret1
 * }</pre>
 * As shown on the diagram, the method {@link #readAsync(int, AsyncCompletionHandler)} runs concurrently with
 * itself in the example above. However, there are no concurrent pending readers because the second operation
 * is invoked after the first operation has completed reading despite the method has not returned yet.
 */
final class NettyStream implements Stream {
    private static final String READ_HANDLER_NAME = "ReadTimeoutHandler";
    private static final int NO_SCHEDULE_TIMEOUT = -1;
    private final ServerAddress address;
    private final SocketSettings settings;
    private final SslSettings sslSettings;
    private final EventLoopGroup workerGroup;
    private final Class<? extends SocketChannel> socketChannelClass;
    private final ByteBufAllocator allocator;

    private volatile boolean isClosed;
    private volatile Channel channel;

    private final LinkedList<io.netty.buffer.ByteBuf> pendingInboundBuffers = new LinkedList<io.netty.buffer.ByteBuf>();
    private PendingReader pendingReader;
    private Throwable pendingException;

    NettyStream(final ServerAddress address, final SocketSettings settings, final SslSettings sslSettings, final EventLoopGroup workerGroup,
                final Class<? extends SocketChannel> socketChannelClass, final ByteBufAllocator allocator) {
        this.address = address;
        this.settings = settings;
        this.sslSettings = sslSettings;
        this.workerGroup = workerGroup;
        this.socketChannelClass = socketChannelClass;
        this.allocator = allocator;
    }

    @Override
    public ByteBuf getBuffer(final int size) {
        return new NettyByteBuf(allocator.buffer(size, size));
    }

    @Override
    public void open() throws IOException {
        FutureAsyncCompletionHandler<Void> handler = new FutureAsyncCompletionHandler<Void>();
        openAsync(handler);
        handler.get();
    }

    @Override
    public void openAsync(final AsyncCompletionHandler<Void> handler) {
        Queue<SocketAddress> socketAddressQueue;

        try {
            socketAddressQueue = new LinkedList<SocketAddress>(address.getSocketAddresses());
        } catch (Throwable t) {
            handler.failed(t);
            return;
        }

        initializeChannel(handler, socketAddressQueue);
    }

    private void initializeChannel(final AsyncCompletionHandler<Void> handler, final Queue<SocketAddress> socketAddressQueue) {
        if (socketAddressQueue.isEmpty()) {
            handler.failed(new MongoSocketException("Exception opening socket", getAddress()));
        } else {
            SocketAddress nextAddress = socketAddressQueue.poll();

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(workerGroup);
            bootstrap.channel(socketChannelClass);

            bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, settings.getConnectTimeout(MILLISECONDS));
            bootstrap.option(ChannelOption.TCP_NODELAY, true);
            bootstrap.option(ChannelOption.SO_KEEPALIVE, true);

            if (settings.getReceiveBufferSize() > 0) {
                bootstrap.option(ChannelOption.SO_RCVBUF, settings.getReceiveBufferSize());
            }
            if (settings.getSendBufferSize() > 0) {
                bootstrap.option(ChannelOption.SO_SNDBUF, settings.getSendBufferSize());
            }
            bootstrap.option(ChannelOption.ALLOCATOR, allocator);

            bootstrap.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(final SocketChannel ch) {
                    if (sslSettings.isEnabled()) {
                        SSLEngine engine = getSslContext().createSSLEngine(address.getHost(), address.getPort());
                        engine.setUseClientMode(true);
                        SSLParameters sslParameters = engine.getSSLParameters();
                        enableSni(address.getHost(), sslParameters);
                        if (!sslSettings.isInvalidHostNameAllowed()) {
                            enableHostNameVerification(sslParameters);
                        }
                        engine.setSSLParameters(sslParameters);
                        ch.pipeline().addFirst("ssl", new SslHandler(engine, false));
                    }
                    int readTimeout = settings.getReadTimeout(MILLISECONDS);
                    if (readTimeout > 0) {
                        ch.pipeline().addLast(READ_HANDLER_NAME, new ReadTimeoutHandler(readTimeout));
                    }
                    ch.pipeline().addLast(new InboundBufferHandler());
                }
            });
            final ChannelFuture channelFuture = bootstrap.connect(nextAddress);
            channelFuture.addListener(new OpenChannelFutureListener(socketAddressQueue, channelFuture, handler));
        }
    }

    @Override
    public void write(final List<ByteBuf> buffers) throws IOException {
        FutureAsyncCompletionHandler<Void> future = new FutureAsyncCompletionHandler<Void>();
        writeAsync(buffers, future);
        future.get();
    }

    @Override
    public ByteBuf read(final int numBytes) throws IOException {
        return read(numBytes, 0);
    }

    @Override
    public boolean supportsAdditionalTimeout() {
        return true;
    }

    @Override
    public ByteBuf read(final int numBytes, final int additionalTimeout) throws IOException {
        isTrueArgument("additionalTimeout must not be negative", additionalTimeout >= 0);
        FutureAsyncCompletionHandler<ByteBuf> future = new FutureAsyncCompletionHandler<ByteBuf>();
        readAsync(numBytes, future, additionalTimeout);
        return future.get();
    }

    @Override
    public void writeAsync(final List<ByteBuf> buffers, final AsyncCompletionHandler<Void> handler) {
        CompositeByteBuf composite = PooledByteBufAllocator.DEFAULT.compositeBuffer();
        for (ByteBuf cur : buffers) {
            composite.addComponent(true, ((NettyByteBuf) cur).asByteBuf());
        }

        channel.writeAndFlush(composite).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    handler.failed(future.cause());
                } else {
                    handler.completed(null);
                }
            }
        });
    }

    @Override
    public void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler) {
        readAsync(numBytes, handler, 0);
    }

    private void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler, final int additionalTimeout) {
        readAsync(numBytes, handler, additionalTimeout, null);
    }

    private void readAsync(final PendingReader pendingReader) {
        readAsync(pendingReader.numBytes, pendingReader.handler, 0, pendingReader);
    }

    private void readAsync(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler, final int additionalTimeout,
                           final PendingReader pendingReaderToUse) {
        ByteBuf buffer = null;
        Throwable exceptionResult = null;
        TimeoutHandle preparedTimeoutHandle = null;
        synchronized (this) {
            exceptionResult = pendingException;
            if (exceptionResult == null) {
                if (!hasBytesAvailable(numBytes)) {
                    if (pendingReaderToUse == null) {
                        preparedTimeoutHandle = prepareTimeout(additionalTimeout);
                        pendingReader = new PendingReader(numBytes, handler, preparedTimeoutHandle);
                    } else {
                        pendingReader = pendingReaderToUse;
                    }
                } else {
                    CompositeByteBuf composite = allocator.compositeBuffer(pendingInboundBuffers.size());
                    int bytesNeeded = numBytes;
                    for (Iterator<io.netty.buffer.ByteBuf> iter = pendingInboundBuffers.iterator(); iter.hasNext();) {
                        io.netty.buffer.ByteBuf next = iter.next();
                        int bytesNeededFromCurrentBuffer = Math.min(next.readableBytes(), bytesNeeded);
                        if (bytesNeededFromCurrentBuffer == next.readableBytes()) {
                            composite.addComponent(next);
                            iter.remove();
                        } else {
                            next.retain();
                            composite.addComponent(next.readSlice(bytesNeededFromCurrentBuffer));
                        }
                        composite.writerIndex(composite.writerIndex() + bytesNeededFromCurrentBuffer);
                        bytesNeeded -= bytesNeededFromCurrentBuffer;
                        if (bytesNeeded == 0) {
                            break;
                        }
                    }
                    buffer = new NettyByteBuf(composite).flip();
                }
            }
        }

        if (preparedTimeoutHandle != null) {
            preparedTimeoutHandle.scheduleTimeout();
        }

        if (exceptionResult != null) {
            if (pendingReaderToUse != null) {
                pendingReaderToUse.timeoutHandle.cancel();
            }
            handler.failed(exceptionResult);
        }
        if (buffer != null) {
            if (pendingReaderToUse != null) {
                pendingReaderToUse.timeoutHandle.cancel();
            }
            handler.completed(buffer);
        }
    }

    private boolean hasBytesAvailable(final int numBytes) {
        int bytesAvailable = 0;
        for (io.netty.buffer.ByteBuf cur : pendingInboundBuffers) {
            bytesAvailable += cur.readableBytes();
            if (bytesAvailable >= numBytes) {
                return true;
            }
        }
        return false;
    }

    private void handleReadResponse(final io.netty.buffer.ByteBuf buffer, final Throwable t) {
        PendingReader localPendingReader = null;
        synchronized (this) {
            if (buffer != null) {
                pendingInboundBuffers.add(buffer.retain());
            } else {
                pendingException = t;
            }
            if (pendingReader != null) {
                localPendingReader = pendingReader;
                pendingReader = null;
            }
        }

        if (localPendingReader != null) {
            readAsync(localPendingReader);
        }
    }

    @Override
    public ServerAddress getAddress() {
        return address;
    }

    @Override
    public synchronized void close() {
        isClosed = true;
        if (channel != null) {
            channel.close();
            channel = null;
        }
        for (Iterator<io.netty.buffer.ByteBuf> iterator = pendingInboundBuffers.iterator(); iterator.hasNext();) {
            io.netty.buffer.ByteBuf nextByteBuf = iterator.next();
            iterator.remove();
            nextByteBuf.release();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    public SocketSettings getSettings() {
        return settings;
    }

    public SslSettings getSslSettings() {
        return sslSettings;
    }

    public EventLoopGroup getWorkerGroup() {
        return workerGroup;
    }

    public Class<? extends SocketChannel> getSocketChannelClass() {
        return socketChannelClass;
    }

    public ByteBufAllocator getAllocator() {
        return allocator;
    }

    private SSLContext getSslContext() {
        try {
            return (sslSettings.getContext() == null) ? SSLContext.getDefault() : sslSettings.getContext();
        } catch (NoSuchAlgorithmException e) {
            throw new MongoClientException("Unable to create default SSLContext", e);
        }
    }

    private class InboundBufferHandler extends SimpleChannelInboundHandler<io.netty.buffer.ByteBuf> {
        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final io.netty.buffer.ByteBuf buffer) throws Exception {
            handleReadResponse(buffer, null);
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable t) {
            if (t instanceof ReadTimeoutException) {
                handleReadResponse(null, new MongoSocketReadTimeoutException("Timeout while receiving message", address, t));
            } else {
                handleReadResponse(null, t);
            }
            ctx.close();
        }
    }

    private static final class PendingReader {
        private final int numBytes;
        private final AsyncCompletionHandler<ByteBuf> handler;
        private final TimeoutHandle timeoutHandle;

        private PendingReader(final int numBytes, final AsyncCompletionHandler<ByteBuf> handler, final TimeoutHandle timeoutHandle) {
            this.numBytes = numBytes;
            this.handler = handler;
            this.timeoutHandle = timeoutHandle;
        }
    }

    private static final class FutureAsyncCompletionHandler<T> implements AsyncCompletionHandler<T> {
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile T t;
        private volatile Throwable throwable;

        FutureAsyncCompletionHandler() {
        }

        @Override
        public void completed(final T t) {
            this.t = t;
            latch.countDown();
        }

        @Override
        public void failed(final Throwable t) {
            this.throwable = t;
            latch.countDown();
        }

        public T get() throws IOException {
            try {
                latch.await();
                if (throwable != null) {
                    if (throwable instanceof IOException) {
                        throw (IOException) throwable;
                    } else if (throwable instanceof MongoException) {
                        throw (MongoException) throwable;
                    } else {
                        throw new MongoInternalException("Exception thrown from Netty Stream", throwable);
                    }
                }
                return t;
            } catch (InterruptedException e) {
                throw new MongoInterruptedException("Interrupted", e);
            }
        }
    }

    private class OpenChannelFutureListener implements ChannelFutureListener {
        private final Queue<SocketAddress> socketAddressQueue;
        private final ChannelFuture channelFuture;
        private final AsyncCompletionHandler<Void> handler;

        OpenChannelFutureListener(final Queue<SocketAddress> socketAddressQueue, final ChannelFuture channelFuture,
                                  final AsyncCompletionHandler<Void> handler) {
            this.socketAddressQueue = socketAddressQueue;
            this.channelFuture = channelFuture;
            this.handler = handler;
        }

        @Override
        public void operationComplete(final ChannelFuture future) {
            synchronized (NettyStream.this) {
                if (future.isSuccess()) {
                    if (isClosed) {
                        channelFuture.channel().close();
                    } else {
                        channel = channelFuture.channel();
                        channel.closeFuture().addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(final ChannelFuture future) {
                                handleReadResponse(null, new IOException("The connection to the server was closed"));
                            }
                        });
                    }
                    handler.completed(null);
                } else {
                    if (isClosed) {
                        handler.completed(null);
                    } else if (socketAddressQueue.isEmpty()) {
                        handler.failed(new MongoSocketOpenException("Exception opening socket", getAddress(), future.cause()));
                    } else {
                        initializeChannel(handler, socketAddressQueue);
                    }
                }
            }
        }
    }

    private void scheduleReadTimeout(final int additionalTimeout) {
        adjustTimeout(false, additionalTimeout);
    }

    private void disableReadTimeout() {
        adjustTimeout(true, 0);
    }

    private void adjustTimeout(final boolean disable, final int additionalTimeout) {
            if (isClosed) {
                return;
            }
            ChannelHandler timeoutHandler = channel.pipeline().get(READ_HANDLER_NAME);
            if (timeoutHandler != null) {
                final ReadTimeoutHandler readTimeoutHandler = (ReadTimeoutHandler) timeoutHandler;
                if (disable) {
                    readTimeoutHandler.removeTimeout();
                } else {
                    readTimeoutHandler.scheduleTimeout(additionalTimeout);
                }
            }
    }

    private TimeoutHandle prepareTimeout(final int additionalTimeout) {
        return new SimpleTimeoutHandle(additionalTimeout);
    }

    private final class SimpleTimeoutHandle implements TimeoutHandle {
        private final int additionalTimeout;

        private boolean cancelled = false;
        private boolean scheduled = false;

        private SimpleTimeoutHandle(final int additionalTimeout) {
            this.additionalTimeout = additionalTimeout;
        }

        @Override
        public synchronized void scheduleTimeout() {
            assert !scheduled : "Attempted to schedule an already scheduled timeout";

            if (!cancelled) {
                scheduleReadTimeout(additionalTimeout);
                scheduled = true;
            }
        }

        @Override
        public synchronized void cancel() {
            assert !cancelled : "Attempted to cancel an already cancelled timeout";

            if (scheduled) {
                disableReadTimeout();
                scheduled = false;
            }
            cancelled = true;
        }
    }

    private interface TimeoutHandle {
        void scheduleTimeout();

        void cancel();
    }
}
