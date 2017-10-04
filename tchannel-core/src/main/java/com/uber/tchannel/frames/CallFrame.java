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
package com.uber.tchannel.frames;

import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.codecs.CodecUtils;
import com.uber.tchannel.codecs.TFrame;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

public abstract class CallFrame extends Frame implements ByteBufHolder {

    public static final byte MORE_FRAGMENTS_REMAIN_MASK = (byte) 0x01;
    public static final short MAX_ARG1_LENGTH = 16384;

    protected byte flags = 0;
    protected ByteBuf payload = null;
    protected ChecksumType checksumType = ChecksumType.NoChecksum;
    protected int checksum = 0;

    public final byte getFlags() {
        return this.flags;
    }

    public boolean moreFragmentsFollow() {
        return ((flags & MORE_FRAGMENTS_REMAIN_MASK) == 1);
    }

    public ChecksumType getChecksumType() {
        return checksumType;
    }

    public int getChecksum() {
        return checksum;
    }

    public final ByteBuf getPayload() {
        return payload;
    }

    public final void setPayload(ByteBuf payload) {
        this.payload = payload;
    }

    public final int getPayloadSize() {
        return this.payload.writerIndex() - this.payload.readerIndex();
    }

    public final boolean isPayloadFull() {
        return getPayloadSize() + TFrame.FRAME_SIZE_LENGTH >= TFrame.MAX_FRAME_PAYLOAD_LENGTH;
    }

    public final ByteBuf encodePayload(ByteBufAllocator allocator) {
        List<ByteBuf> args = new ArrayList<>();
        args.add(Unpooled.EMPTY_BUFFER);
        args.add(Unpooled.EMPTY_BUFFER);
        args.add(Unpooled.EMPTY_BUFFER);
        return encodePayload(allocator, args);
    }

    public final ByteBuf encodePayload(@NotNull ByteBufAllocator allocator, @NotNull List<ByteBuf> args) {
        ByteBuf payload = CodecUtils.writeArgs(allocator, encodeHeader(allocator), args);

        if (args.isEmpty()) {
            this.flags = 0;
            payload.setByte(0, 0);
        } else {
            this.flags = 1;
            payload.setByte(0, 1);
        }

        this.payload = payload;
        return payload;
    }

    @Override
    public ByteBuf content() {
        return this.payload;
    }

    @Override
    public ByteBufHolder retain() {
        this.payload.retain();
        return this;
    }

    @Override
    public ByteBufHolder retain(int i) {
        this.payload.retain(i);
        return this;
    }

    @Override
    public ByteBufHolder touch() {
        this.payload.touch();
        return this;
    }

    @Override
    public ByteBufHolder touch(Object o) {
        this.payload.touch(o);
        return this;
    }

    @Override
    public int refCnt() {
        return this.payload.refCnt();
    }

    @Override
    public boolean release() {
        return this.payload.release();
    }

    @Override
    public boolean release(int i) {
        return this.payload.release(i);
    }

    @Override
    public ByteBufHolder copy() {
        return replace(this.payload.copy());
    }

    @Override
    public ByteBufHolder duplicate() {
        return replace(this.payload.duplicate());
    }

    @Override
    public ByteBufHolder retainedDuplicate() {
        return replace(this.payload.retainedDuplicate());
    }

}
