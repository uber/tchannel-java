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

import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.fragmentation.FragmentationState;
import com.uber.tchannel.codecs.TFrame;
import com.uber.tchannel.frames.CallRequestFrame;
import com.uber.tchannel.frames.CallResponseFrame;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.messages.RawMessage;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class MessageFragmenter extends MessageToMessageEncoder<RawMessage> {

    private static final int DEFAULT_BUFFER_SIZE = 1024;
    private static final int MAX_BUFFER_SIZE = TFrame.MAX_FRAME_LENGTH - TFrame.FRAME_HEADER_LENGTH;

    @Override
    protected void encode(ChannelHandlerContext ctx, RawMessage msg, List<Object> out) throws Exception {

        FragmentationState state = FragmentationState.ARG1;
        while (state != FragmentationState.DONE) {
            state = this.sendOutbound(ctx, msg, state, out);
        }

        if (msg instanceof Response) {
            ((Response)msg).release();
        }
    }

    protected void writeOutbound(ChannelHandlerContext ctx,
                                 ByteBuf buffer,
                                 RawMessage msg,
                                 FragmentationState state,
                                 List<Object> out) {
        byte flags = 0x01;
        if (state == FragmentationState.DONE) {
            flags = 0x00;
        }

        if (msg instanceof Request) {
            Request request = (Request) msg;
            CallRequestFrame callRequestFrame = new CallRequestFrame(
                request.getId(),
                flags,
                request.getTTL(),
                new Trace(0, 0, 0, (byte) 0x00),
                request.getService(),
                request.getTransportHeaders(),
                ChecksumType.NoChecksum,
                0,
                buffer
            );

            out.add(callRequestFrame);
        } else if (msg instanceof Response) {
            Response response = (Response) msg;
            CallResponseFrame callResponseFrame = new CallResponseFrame(
                response.getId(),
                flags,
                ResponseCode.OK,
                new Trace(0, 0, 0, (byte) 0x00),
                response.getTransportHeaders(),
                ChecksumType.NoChecksum,
                0,
                buffer
            );

            out.add(callResponseFrame);
        }

    }

    protected FragmentationState sendOutbound(ChannelHandlerContext ctx,
                                              RawMessage msg,
                                              FragmentationState state,
                                              List<Object> out) {

        ByteBuf buffer = ctx.alloc().buffer(DEFAULT_BUFFER_SIZE, MAX_BUFFER_SIZE);

        while (true) {
            switch (state) {
                case ARG1:
                    this.writeArg(msg.getArg1(), buffer);
                    state = FragmentationState.nextState(state);
                    break;

                case ARG2:
                    if (this.writeArg(msg.getArg2(), buffer) > 0) {
                        this.writeOutbound(ctx, buffer, msg, state, out);
                        return state;
                    }
                    state = FragmentationState.nextState(state);
                    break;

                case ARG3:
                    if (this.writeArg(msg.getArg3(), buffer) > 0) {
                        this.writeOutbound(ctx, buffer, msg, state, out);
                        return state;
                    }
                    state = FragmentationState.nextState(state);
                    break;

                case DONE:
                default:
                    writeOutbound(ctx, buffer, msg, state, out);
                    return state;

            }
        }

    }

    protected int writeArg(ByteBuf arg, ByteBuf buffer) {

        int readableBytes = arg.readableBytes();
        int writableBytes = buffer.maxWritableBytes();
        int headerSize = 2;
        int chunkLength = Math.min(readableBytes + headerSize, writableBytes);

        // Write the size of the `arg`
        buffer.writeShort(chunkLength - headerSize);

        // Actually write the contents of `arg`
        buffer.writeBytes(arg, chunkLength - headerSize);

        // Release the arg back to the pool
        int bytesRemaining = (readableBytes - (chunkLength - headerSize));

        return bytesRemaining;
    }
}
