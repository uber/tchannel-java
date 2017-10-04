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

import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.codecs.MessageCodec;
import com.uber.tchannel.frames.CallFrame;
import com.uber.tchannel.frames.CallRequestContinueFrame;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallResponseContinueFrame;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.frames.FrameType;
import com.uber.tchannel.messages.RawMessage;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.tracing.Trace;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public class MessageFragmenter extends MessageToMessageEncoder<RawMessage> {

    @Override
    protected void encode(ChannelHandlerContext ctx, RawMessage msg, List<Object> out) throws Exception {
        writeFrames(ctx, msg, out);
        if (msg instanceof Response) {
            msg.release();
        }
    }

    protected void writeFrames(
        @NotNull ChannelHandlerContext ctx,
        @NotNull RawMessage msg,
        @NotNull List<Object> frames
    ) throws Exception {
        List<ByteBuf> args = new ArrayList<>(3);
        args.add(msg.getArg1());
        args.add(msg.getArg2());
        args.add(msg.getArg3());

        CallFrame frame = null;
        while (!args.isEmpty()) {
            if (frame == null || frame.isPayloadFull()) {
                frame = createFrame(msg, args.size());
            }

            frame.encodePayload(ctx.alloc(), args);
            frames.add(
                MessageCodec.encode(ctx.alloc(),
                    MessageCodec.encode(
                        ctx.alloc(),
                        frame
                    )
                )
            );
        }
    }

    protected @NotNull CallFrame createFrame(@NotNull RawMessage msg, int argCount) {
        if (msg.getType() == FrameType.CallRequest) {
            Request request = (Request) msg;
            if (argCount == 3) {
                return new CallRequestFrame(
                    request.getId(),
                    (byte)0,
                    request.getTTL(),
                    new Trace(0, 0, 0, (byte) 0x00),
                    request.getService(),
                    request.getTransportHeaders(),
                    ChecksumType.NoChecksum,
                    0,
                    TChannelUtilities.emptyByteBuf
                );
            } else {
                return new CallRequestContinueFrame(
                    request.getId(),
                    (byte)0,
                    ChecksumType.NoChecksum,
                    0,
                    TChannelUtilities.emptyByteBuf
                );
            }
        } else {
            Response response = (Response) msg;
            if (argCount == 3) {
                return new CallResponseFrame(
                    response.getId(),
                    (byte)0,
                    response.getResponseCode(),
                    new Trace(0, 0, 0, (byte) 0x00),
                    response.getTransportHeaders(),
                    ChecksumType.NoChecksum,
                    0,
                    TChannelUtilities.emptyByteBuf
                );
            } else {
                return new CallResponseContinueFrame(
                    response.getId(),
                    (byte)0,
                    ChecksumType.NoChecksum,
                    0,
                    TChannelUtilities.emptyByteBuf
                );
            }
        }
    }

}
