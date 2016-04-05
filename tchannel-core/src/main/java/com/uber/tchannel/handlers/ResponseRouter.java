/*
 * Copyright (c) 2015 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.uber.tchannel.handlers;

import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.errors.TChannelConnectionReset;
import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.ErrorResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.ResponseMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ResponseRouter extends SimpleChannelInboundHandler<ResponseMessage> {
    private static final Logger logger = LoggerFactory.getLogger(ResponseRouter.class);

    private final PeerManager peerManager;
    private final HashedWheelTimer timer;
    private AtomicBoolean destroyed = new AtomicBoolean(false);

    private final int resetOnTimeoutLimit;
    private AtomicInteger timeouts = new AtomicInteger(0);

    private final AtomicBoolean busy = new AtomicBoolean(false);
    private final ConcurrentLinkedQueue<Long> requestQueue = new ConcurrentLinkedQueue<Long>();
    private final Map<Long, OutRequest> requestMap = new ConcurrentHashMap<>();
    private final int maxPendingRequests;

    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private ChannelHandlerContext ctx;

    public ResponseRouter(TChannel topChannel, HashedWheelTimer timer) {
        this.peerManager = topChannel.getPeerManager();
        this.resetOnTimeoutLimit = topChannel.getResetOnTimeoutLimit();
        this.timer = timer;
        this.maxPendingRequests = topChannel.getClientMaxPendingRequests();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        sendRequest();
    }

    protected void sendRequest() {

        if (!busy.compareAndSet(false, true)) {
            return;
        }

        Channel channel = ctx.channel();
        try {
            boolean flush = false;
            while (!requestQueue.isEmpty() && channel.isWritable()) {
                long id = requestQueue.poll();
                OutRequest outRequest = requestMap.get(id);
                if (outRequest != null) {
                    outRequest.setChannelFuture(channel.write(outRequest.getRequest()));
                    flush = true;
                }
            }

            if (flush) {
                channel.flush();
            }
        } finally {
            busy.set(false);
        }

        // in case there are new request added
        if (channel.isWritable() && !requestQueue.isEmpty()) {
            sendRequest();
        }
    }

    public boolean expectResponse(OutRequest outRequest) {
        int messageId = idGenerator.incrementAndGet();
        Request request = outRequest.getRequest();
        request.setId(messageId);
        if (this.destroyed.get()) {
            outRequest.setLastError(ErrorType.NetworkError, "Connection already closed");
            return false;
        } else if (this.requestMap.size() + requestQueue.size() > maxPendingRequests) {
            outRequest.setLastError(ErrorType.Busy,
                String.format("Client max pending request limit of %d is reached", maxPendingRequests));
            return false;
        }

        return send(outRequest);
    }

    protected boolean send(OutRequest outRequest) {
        Request request = outRequest.getRequest();
        this.requestMap.put(request.getId(), outRequest);
        setTimer(outRequest);

        if (!ctx.channel().isActive()) {
            handleResponse(new ErrorResponse(
                outRequest.getRequest().getId(),
                ErrorType.NetworkError,
                "Channel is closed"));
            return false;
        }

        requestQueue.offer(outRequest.getRequest().getId());
        sendRequest();

        return true;
    }

    protected void setTimer(final OutRequest outRequest) {
        final long start = System.currentTimeMillis();
        Timeout timeout = timer.newTimeout(new TimerTask() {
            @Override
            public void run(Timeout timeout) throws Exception {
                // prevent ByteBuf refCnt leak
                outRequest.flushWrite();
                if (timeouts.incrementAndGet() >= resetOnTimeoutLimit) {
                    // reset on continuous timeouts
                    peerManager.handleConnectionErrors(ctx.channel(),
                        new TChannelConnectionReset(String.format(
                            "Connection reset due to continuous %d timeouts", resetOnTimeoutLimit)));
                    return;
                }

                handleResponse(new ErrorResponse(
                    outRequest.getRequest().getId(),
                    ErrorType.Timeout,
                    String.format("Request timeout after %dms", System.currentTimeMillis() - start)));
            }
        }, outRequest.getRequest().getTimeout(), TimeUnit.MILLISECONDS);

        outRequest.setTimeout(timeout);
    }

    protected void handleResponse(ResponseMessage response) {
        OutRequest outRequest = this.requestMap.remove(response.getId());

        // this may happen when the request times out already
        if (outRequest == null) {
            response.release();
            return;
        }

        outRequest.handleResponse(response);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ResponseMessage response) {
        handleResponse(response);
    }

    public void clean() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }

        Set<Long> keys = requestMap.keySet();
        for (long key : keys) {
            OutRequest outRequest = requestMap.remove(key);
            if (outRequest == null) {
                continue;
            }

            // wait until the send is completed
            outRequest.flushWrite();

            // Complete the request
            outRequest.setLastError(ErrorType.NetworkError,
                "Connection was reset due to network error");
            outRequest.setFuture();
        }

        for (long key : requestQueue) {
            OutRequest outRequest = requestMap.remove(key);
            if (outRequest == null) {
                continue;
            }

            // wait until the send is completed
            outRequest.flushWrite();

            // Complete the request
            outRequest.setLastError(ErrorType.NetworkError,
                "Connection was reset due to network error");
            outRequest.setFuture();
        }

    }
}
