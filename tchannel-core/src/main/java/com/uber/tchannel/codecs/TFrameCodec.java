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
package com.uber.tchannel.codecs;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;

import java.util.List;

public final class TFrameCodec extends ByteToMessageCodec<TFrame> {

    @Override
    protected void encode(ChannelHandlerContext ctx, TFrame frame, ByteBuf out) throws Exception {
        // size:2
        out.writeShort(frame.size + TFrame.FRAME_HEADER_LENGTH);

        // type:1
        out.writeByte(frame.type);

        // reserved:1
        out.writeZero(1);

        // id:4
        out.writeInt((int) frame.id);

        // reserved:8
        out.writeZero(8);

        // payload:16+
        out.writeBytes(frame.payload);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        msg.retain();
        out.add(decode(msg));
    }

    public static ByteBuf encode(ByteBufAllocator allocator, TFrame frame) {
        ByteBuf buffer = allocator.buffer(TFrame.FRAME_HEADER_LENGTH, TFrame.FRAME_HEADER_LENGTH);

        final ByteBuf result;
        boolean release = true;
        try {
            // size:2
            buffer.writeShort(frame.size + TFrame.FRAME_HEADER_LENGTH);

            // type:1
            buffer.writeByte(frame.type);

            // reserved:1
            buffer.writeZero(1);

            // id:4
            buffer.writeInt((int) frame.id);

            // reserved:8
            buffer.writeZero(8);

            // TODO: refactor
            if (frame.payload instanceof CompositeByteBuf) {
                CompositeByteBuf cbf = (CompositeByteBuf) frame.payload;
                cbf.addComponent(0, buffer);
                cbf.writerIndex(cbf.writerIndex() + TFrame.FRAME_HEADER_LENGTH);
                result = cbf;
            } else {
                result = Unpooled.wrappedBuffer(buffer, frame.payload);
            }
            release = false;
        } finally {
            if (release) {
                buffer.release();
            }
        }
        return result;
    }

    public static TFrame decode(ByteBuf buffer) {
        // size:2
        int size = buffer.readUnsignedShort() - TFrame.FRAME_HEADER_LENGTH;

        // type:1
        byte type = buffer.readByte();

        // reserved:1
        buffer.skipBytes(1);

        // id:4
        long id = buffer.readUnsignedInt();

        // reserved:8
        buffer.skipBytes(8);

        // payload:16+
        ByteBuf payload = buffer.readSlice(size);
//        payload.retain();

        return new TFrame(size, type, id, payload);
    }
}
