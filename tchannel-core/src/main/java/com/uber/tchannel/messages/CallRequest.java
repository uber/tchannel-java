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

/**
 * This is the primary RPC mechanism. The triple of (arg1, arg2, arg3) is sent to "service" via the remote end
 * of this connection.
 * <p>
 * Whether connecting directly to a service or through a service router, the service name is always specified.
 * This supports an explicit router model as well as peers electing to delegate some requests to another service.
 * <p>
 * A forwarding intermediary can relay payloads without understanding the contents of the args triple.
 * <p>
 * A {@link CallRequest} may be fragmented across multiple frames. If so, the first frame is a {@link CallRequest},
 * and all subsequent frames are {@link CallRequestContinue} frames.
 * <p>
 * The size of arg1 is at most 16KiB.
 */
public final class CallRequest implements Message, CallMessage {

    private final long id;
    private final byte flags;
    private final long ttl;
    private final Trace tracing;
    private final String service;
    private final Map<String, String> headers;
    private final ChecksumType checksumType;
    private final int checksum;
    private final ByteBuf payload;

    public CallRequest(long id, byte flags, long ttl, Trace tracing, String service, Map<String, String> headers,
                       ChecksumType checksumType, int checksum, ByteBuf payload) {
        this.id = id;
        this.flags = flags;
        this.ttl = ttl;
        this.tracing = tracing;
        this.service = service;
        this.headers = headers;
        this.checksumType = checksumType;
        this.checksum = checksum;
        this.payload = payload;
    }

    public byte getFlags() {
        return flags;
    }

    public boolean moreFragmentsFollow() {
        return ((this.flags & CallMessage.MORE_FRAGMENTS_REMAIN_MASK) == 1);
    }

    public ChecksumType getChecksumType() {
        return this.checksumType;
    }

    public int getChecksum() {
        return this.checksum;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    public long getId() {
        return this.id;
    }

    public int getPayloadSize() {
        return this.payload.writerIndex() - this.payload.readerIndex();
    }

    public MessageType getMessageType() {
        return MessageType.CallRequest;
    }

    public long getTTL() {
        return this.ttl;
    }

    public Trace getTracing() {
        return this.tracing;
    }

    public String getService() {
        return this.service;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
    }

    public ByteBuf content() {
        return this.payload;
    }

    public ByteBufHolder copy() {
        return new CallRequest(
                this.id,
                this.flags,
                this.ttl,
                this.tracing,
                this.service,
                this.headers,
                this.checksumType,
                this.checksum,
                this.payload.copy()
        );
    }

    public ByteBufHolder duplicate() {
        return new CallRequest(
                this.id,
                this.flags,
                this.ttl,
                this.tracing,
                this.service,
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
}
