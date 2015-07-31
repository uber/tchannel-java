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

public final class CallRequestContinue implements Message, CallMessage {

    private final long id;
    private final byte flags;
    private final ChecksumType checksumType;
    private final int checksum;
    private final ByteBuf payload;

    public CallRequestContinue(long id, byte flags, ChecksumType checksumType, int checksum, ByteBuf payload) {
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
        return flags;
    }

    public ChecksumType getChecksumType() {
        return checksumType;
    }

    public int getChecksum() {
        return checksum;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    public boolean moreFragmentsFollow() {
        return ((this.flags & CallMessage.MORE_FRAGMENTS_REMAIN_MASK) == 1);
    }

    public long getId() {
        return this.id;
    }

    public MessageType getMessageType() {
        return MessageType.CallRequestContinue;
    }

}
