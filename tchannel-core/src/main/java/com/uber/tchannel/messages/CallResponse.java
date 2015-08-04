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
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;

import java.util.Map;
import java.util.Optional;

/**
 * Very similar to {@link CallRequest}, differing only in: adds a code field, no ttl field and no service field.
 * <p>
 * All common fields have identical definition to {@link CallRequest}. It is not necessary for arg1 to have the same
 * value between the {@link CallRequest} and the {@link CallResponse}; by convention, existing implementations leave
 * arg1 at zero length for {@link CallResponse} messages.
 * <p>
 * The size of arg1 is at most 16KiB.
 */
public final class CallResponse implements Message, CallMessage {

    private final long id;
    private final byte flags;
    private final CallResponseCode code;
    private final Trace tracing;
    private final Map<String, String> headers;
    private final ChecksumType checksumType;
    private final int checksum;
    private final ByteBuf payload;

    public CallResponse(long id, byte flags, CallResponseCode code, Trace tracing, Map<String, String> headers,
                        ChecksumType checksumType, int checksum, ByteBuf payload) {
        this.id = id;
        this.flags = flags;
        this.code = code;
        this.tracing = tracing;
        this.headers = headers;
        this.checksumType = checksumType;
        this.checksum = checksum;
        this.payload = payload;
    }

    public byte getFlags() {
        return this.flags;
    }

    public int getPayloadSize() {
        return this.payload.writerIndex() - this.payload.readerIndex();
    }

    public boolean ok() {
        return (this.code == CallResponseCode.OK);
    }

    public boolean moreFragmentsFollow() {
        return ((this.flags & CallMessage.MORE_FRAGMENTS_REMAIN_MASK) == 1);
    }

    public MessageType getMessageType() {
        return MessageType.CallResponse;
    }

    public long getId() {
        return this.id;
    }

    public Trace getTracing() {
        return this.tracing;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
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

    public CallResponseCode getCode() {
        return code;
    }

    public ByteBuf content() {
        return this.payload;
    }

    public ByteBufHolder copy() {
        return new CallResponse(
                this.id,
                this.flags,
                this.code,
                this.tracing,
                this.headers,
                this.checksumType,
                this.checksum,
                this.payload.copy()
        );
    }

    public ByteBufHolder duplicate() {
        return new CallResponse(
                this.id,
                this.flags,
                this.code,
                this.tracing,
                this.headers,
                this.checksumType,
                this.checksum,
                this.payload.duplicate()
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

    public enum CallResponseCode {
        OK((byte) 0x00),
        Error((byte) 0x01);

        private final byte code;

        CallResponseCode(byte code) {
            this.code = code;
        }

        public static Optional<CallResponseCode> fromByte(byte code) {
            switch (code) {
                case 0x00:
                    return Optional.of(OK);
                case 0x01:
                    return Optional.of(Error);
                default:
                    return Optional.empty();
            }
        }

        public byte byteValue() {
            return this.code;
        }
    }

}
