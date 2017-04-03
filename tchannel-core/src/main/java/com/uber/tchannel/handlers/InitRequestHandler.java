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
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.frames.Frame;
import com.uber.tchannel.frames.InitFrame;
import com.uber.tchannel.frames.InitRequestFrame;
import com.uber.tchannel.frames.InitResponseFrame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import static com.uber.tchannel.frames.ErrorFrame.sendError;

public class InitRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final PeerManager peerManager;

    public InitRequestHandler(PeerManager peerManager) {
        this.peerManager = peerManager;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {

        Frame frame = MessageCodec.decode(
            MessageCodec.decode(buf)
        );

        switch (frame.getType()) {

            case InitRequest:

                InitRequestFrame initRequestFrameMessage = (InitRequestFrame) frame;

                if (initRequestFrameMessage.getVersion() == InitFrame.DEFAULT_VERSION) {

                    InitResponseFrame initResponseFrame = new InitResponseFrame(
                        initRequestFrameMessage.getId(),
                        InitFrame.DEFAULT_VERSION
                    );

                    initResponseFrame.setHostPort(this.peerManager.getHostPort());

                    // TODO: figure out what to put here
                    initResponseFrame.setProcessName("java-process");
                    MessageCodec.write(ctx, initResponseFrame);
                    ctx.pipeline().remove(this);
                    peerManager.setIdentified(ctx.channel(), initRequestFrameMessage.getHeaders());

                } else {
                    sendError(ErrorType.FatalProtocolError,
                        String.format("Expected Protocol version: %d, got version: %d", InitFrame.DEFAULT_VERSION,
                            initRequestFrameMessage.getVersion()),
                        frame.getId(), ctx);
                }

                break;

            default:
                sendError(ErrorType.FatalProtocolError,
                    "The first frame should be an Init Request",
                    frame.getId(), ctx);
                break;
        }
    }
}
