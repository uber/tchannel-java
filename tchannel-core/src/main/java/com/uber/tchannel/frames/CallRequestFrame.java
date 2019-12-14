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
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
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
 * A {@link CallRequestFrame} may be fragmented across multiple frames. If so, the first frame is a
 * {@link CallRequestFrame}, and all subsequent frames are {@link CallRequestContinueFrame} frames.
 * <p>
 * The size of arg1 is at most 16KiB.
 */
public class CallRequestFrame extends CallFrame {

    private long ttl;
    private Trace tracing;
    private String service;
    private Map<String, String> headers;

    public CallRequestFrame(long id, byte flags, long ttl, Trace tracing, String service, Map<String, String> headers,
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

    public CallRequestFrame(long id, long ttl, Trace tracing, String service, Map<String, String> headers,
                            ChecksumType checksumType, int checksum) {
        this(id, (byte)0, ttl, tracing, service, headers, checksumType, checksum, null);
    }

    protected CallRequestFrame(long id) {
        this(id, (byte)0, 0L, null, null, null, null, 0, null);
    }

    @Override
    public FrameType getType() {
        return FrameType.CallRequest;
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

    public ArgScheme getArgScheme() {
        return ArgScheme.toScheme(headers.get(TransportHeaders.ARG_SCHEME_KEY));
    }

    @Override
    public ByteBuf encodeHeader(ByteBufAllocator allocator) {
        ByteBuf buffer = allocator.buffer(1024);

        boolean release = true;
        try {
            // flags:1
            buffer.writeByte(getFlags());

            // ttl:4
            buffer.writeInt((int) getTTL());

            // tracing:25
            CodecUtils.encodeTrace(getTracing(), buffer);

            // service~1
            CodecUtils.encodeSmallString(getService(), buffer);

            // nh:1 (hk~1, hv~1){nh}
            CodecUtils.encodeSmallHeaders(getHeaders(), buffer);

            // csumtype:1
            buffer.writeByte(getChecksumType().byteValue());

            // (csum:4){0,1}
            CodecUtils.encodeChecksum(getChecksum(), getChecksumType(), buffer);
            release = false;
        } finally {
            if (release) {
                buffer.release();
            }
        }
        return buffer;
    }

    @Override
    public void decode(TFrame tFrame) {

        // flags:1
        flags = tFrame.payload.readByte();

        // ttl:4
        ttl = tFrame.payload.readUnsignedInt();

        // tracing:25
        tracing = CodecUtils.decodeTrace(tFrame.payload);

        // service~1
        service = CodecUtils.decodeSmallString(tFrame.payload);

        // nh:1 (hk~1, hv~1){nh}
        headers = CodecUtils.decodeSmallHeaders(tFrame.payload);

        // csumtype:1
        checksumType = ChecksumType.fromByte(tFrame.payload.readByte());

        // (csum:4){0,1}
        checksum = CodecUtils.decodeChecksum(checksumType, tFrame.payload);

        // arg1~2 arg2~2 arg3~2
        int payloadSize = tFrame.size - tFrame.payload.readerIndex();
        payload = tFrame.payload.readSlice(payloadSize);
    }

    @Override
    public ByteBufHolder replace(ByteBuf payload) {
        return new CallRequestFrame(this.id, this.flags, this.ttl, this.tracing,
                this.service, this.headers, this.checksumType, this.checksum, payload);
    }
}
