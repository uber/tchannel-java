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
public class CallRequest implements Message, CallMessage {

    private final long id;
    private final byte flags;
    private final long ttl;
    private final Trace tracing;
    private final String service;
    private final Map<String, String> headers;
    private final ChecksumType checksumType;
    private final int checksum;
    private final ByteBuf arg1;
    private final ByteBuf arg2;
    private final ByteBuf arg3;

    public CallRequest(long id, byte flags, long ttl, Trace tracing, String service,
                       Map<String, String> headers, ChecksumType checksumType, int checksum,
                       ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        this.id = id;
        this.flags = flags;
        this.ttl = ttl;
        this.tracing = tracing;
        this.service = service;
        this.headers = headers;
        this.checksumType = checksumType;
        this.checksum = checksum;
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
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

    public ByteBuf getArg1() {
        return this.arg1;
    }

    public ByteBuf getArg2() {
        return this.arg2;
    }

    public ByteBuf getArg3() {
        return this.arg3;
    }

    public long getId() {
        return this.id;
    }

    public MessageType getMessageType() {
        return MessageType.CallRequest;
    }

    public long getTtl() {
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

}
