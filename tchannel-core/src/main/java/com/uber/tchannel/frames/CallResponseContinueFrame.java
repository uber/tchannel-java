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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;

public final class CallResponseContinueFrame extends CallFrame {

    public CallResponseContinueFrame(long id, byte flags, ChecksumType checksumType, int checksum, ByteBuf payload) {
        this.id = id;
        this.flags = flags;
        this.checksumType = checksumType;
        this.checksum = checksum;
        this.payload = payload;
    }

    public CallResponseContinueFrame(long id, ChecksumType checksumType, int checksum) {
        this.id = id;
        this.checksumType = checksumType;
        this.checksum = checksum;
    }


    public FrameType getType() {
        return FrameType.CallResponseContinue;
    }

    public ByteBufHolder copy() {
        return new CallResponseContinueFrame(
                this.id,
                this.flags,
                this.checksumType,
                this.checksum,
                this.payload.copy()
        );
    }

    public ByteBufHolder duplicate() {
        return new CallResponseContinueFrame(
                this.id,
                this.flags,
                this.checksumType,
                this.checksum,
                this.payload.copy()
        );
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
}
