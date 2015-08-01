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
package com.uber.tchannel.messages;

import com.uber.tchannel.checksum.ChecksumType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;

public final class CallResponseContinue implements Message, CallMessage {

    private final long id;
    private final byte flags;
    private final ChecksumType checksumType;
    private final int checksum;
    private final ByteBuf payload;

    public CallResponseContinue(long id, byte flags, ChecksumType checksumType, int checksum, ByteBuf payload) {
        this.id = id;
        this.flags = flags;
        this.checksumType = checksumType;
        this.checksum = checksum;
        this.payload = payload;
    }

    public int getPayloadSize() {
        return this.payload.writerIndex() - this.payload.readerIndex();
    }

    public byte getFlags() {
        return this.flags;
    }

    public ChecksumType getChecksumType() {
        return this.checksumType;
    }

    public int getChecksum() {
        return this.checksum;
    }

    public ByteBuf getPayload() {
        return this.payload;
    }

    public boolean moreFragmentsFollow() {
        return ((this.flags & CallMessage.MORE_FRAGMENTS_REMAIN_MASK) == 1);
    }

    public long getId() {
        return this.id;
    }

    public MessageType getMessageType() {
        return MessageType.CallResponseContinue;
    }

    public ByteBuf content() {
        return this.payload;
    }

    public ByteBufHolder copy() {
        return new CallResponseContinue(
                this.id,
                this.flags,
                this.checksumType,
                this.checksum,
                this.payload.copy()
        );
    }

    public ByteBufHolder duplicate() {
        return new CallResponseContinue(
                this.id,
                this.flags,
                this.checksumType,
                this.checksum,
                this.payload.copy()
        );
    }

    public ByteBufHolder retain() {
        this.payload.retain();
        return this;
    }

    public ByteBufHolder retain(int i) {
        this.payload.retain(i);
        return this;
    }

    public ByteBufHolder touch() {
        this.payload.touch();
        return this;
    }

    public ByteBufHolder touch(Object o) {
        this.payload.touch(o);
        return this;
    }

    public int refCnt() {
        return this.payload.refCnt();
    }

    public boolean release() {
        return this.payload.release();
    }

    public boolean release(int i) {
        return this.payload.release(i);
    }

}
