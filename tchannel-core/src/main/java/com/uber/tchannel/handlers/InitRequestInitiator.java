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
import com.uber.tchannel.frames.InitFrame;
import com.uber.tchannel.frames.InitResponseFrame;
import com.uber.tchannel.frames.Frame;
import com.uber.tchannel.tracing.Trace;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class InitRequestInitiator extends SimpleChannelInboundHandler<Frame> {

    private final PeerManager peerManager;

    public InitRequestInitiator(PeerManager peerManager) {
        this.peerManager = peerManager;
    }

    @Override
    protected void messageReceived(ChannelHandlerContext ctx, Frame frame) throws ProtocolError {

        switch (frame.getMessageType()) {

            case InitResponse:

                InitResponseFrame initResponseFrameMessage = (InitResponseFrame) frame;

                if (initResponseFrameMessage.getVersion() == InitFrame.DEFAULT_VERSION) {
                    ctx.pipeline().remove(this);
                    peerManager.setIdentified(ctx.channel(), initResponseFrameMessage.getHeaders());
                } else {
                    throw new FatalProtocolError(
                            String.format("Expected Protocol version: %d", InitFrame.DEFAULT_VERSION),
                            new Trace(0, 0, 0, (byte) 0x00)
                    );
                }

                break;

            default:

                throw new FatalProtocolError(
                        "Must not send any data until receiving InitFrame Response",
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
