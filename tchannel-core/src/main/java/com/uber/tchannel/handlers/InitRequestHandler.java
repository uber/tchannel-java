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

import com.uber.tchannel.channels.PeerManager;
import com.uber.tchannel.errors.FatalProtocolError;
import com.uber.tchannel.errors.ProtocolError;
import com.uber.tchannel.errors.ProtocolErrorProcessor;
import com.uber.tchannel.messages.InitMessage;
import com.uber.tchannel.messages.InitRequest;
import com.uber.tchannel.messages.InitResponse;
import com.uber.tchannel.messages.Message;
import com.uber.tchannel.tracing.Trace;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class InitRequestHandler extends SimpleChannelInboundHandler<Message> {

    private final PeerManager peerManager;

    public InitRequestHandler(PeerManager peerManager) {
        this.peerManager = peerManager;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, Message message) throws Exception {

        switch (message.getMessageType()) {

            case InitRequest:

                InitRequest initRequestMessage = (InitRequest) message;

                if (initRequestMessage.getVersion() == InitMessage.DEFAULT_VERSION) {
                    InitResponse initResponse = new InitResponse(
                            initRequestMessage.getId(),
                            InitMessage.DEFAULT_VERSION
                    );
                    initResponse.setHostPort(this.peerManager.getHostPort());
                    // TODO: figure out what to put here
                    initResponse.setProcessName("java-process");
                    ChannelFuture f = ctx.writeAndFlush(initResponse);
                    f.addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
                    ctx.pipeline().remove(this);
                    peerManager.setIdentified(ctx.channel(), initRequestMessage.getHeaders());
                } else {
                    // TODO: response ProtocolError
                    throw new FatalProtocolError(
                            String.format("Expected Protocol version: %d", InitMessage.DEFAULT_VERSION),
                            new Trace(0, 0, 0, (byte) 0x00)
                    );
                }

                break;

            default:

                // TODO: should send back ProtocolError
                throw new FatalProtocolError(
                        "Must not send any data until receiving Init Request",
                        new Trace(0, 0, 0, (byte) 0x00)
                );

        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

        if (cause instanceof ProtocolError) {
            ProtocolError protocolError = (ProtocolError) cause;
            ProtocolErrorProcessor.handleError(ctx, protocolError);
        } else {
            super.exceptionCaught(ctx, cause);
        }

    }
}
