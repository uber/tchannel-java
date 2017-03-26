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

public final class CallRequestContinueFrame extends CallFrame {

    public CallRequestContinueFrame(long id, byte flags, ChecksumType checksumType, int checksum, ByteBuf payload) {
        this.id = id;
        this.flags = flags;
        this.checksumType = checksumType;
        this.checksum = checksum;
        this.payload = payload;
    }

    protected CallRequestContinueFrame(long id) {
        this(id, (byte)0, null, 0, null);
    }

    @Override
    public FrameType getType() {
        return FrameType.CallRequestContinue;
    }

    @Override
    public ByteBuf encodeHeader(ByteBufAllocator allocator) {

        ByteBuf buffer = allocator.buffer(2, 6);

        // flags:1
        buffer.writeByte(getFlags());

        // csumtype:1
        buffer.writeByte(getChecksumType().byteValue());

        // checksum -> (csum:4){0,1}
        CodecUtils.encodeChecksum(getChecksum(), getChecksumType(), buffer);

        return buffer;
    }

    @Override
    public void decode(TFrame tFrame) {

        // flags:1
        flags = tFrame.payload.readByte();

        // csumtype:1
        checksumType = ChecksumType.fromByte(tFrame.payload.readByte());

        // (csum:4){0,1}
        checksum = CodecUtils.decodeChecksum(checksumType, tFrame.payload);

        // {continuation}
        int payloadSize = tFrame.size - tFrame.payload.readerIndex();
        payload = tFrame.payload.readSlice(payloadSize);
    }

    @Override
    public ByteBufHolder replace(ByteBuf payload) {
        return new CallRequestContinueFrame(this.id, this.flags, this.checksumType, this.checksum, payload);
    }
}
