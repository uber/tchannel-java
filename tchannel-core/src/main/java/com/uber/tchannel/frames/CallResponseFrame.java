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

import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.codecs.CodecUtils;
import com.uber.tchannel.codecs.TFrame;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufHolder;

import java.util.Map;

/**
 * Very similar to {@link CallRequestFrame}, differing only in: adds a responseCode field, no ttl field and no service
 * field.
 * <p>
 * All common fields have identical definition to {@link CallRequestFrame}. It is not necessary for arg1 to have the
 * same value between the {@link CallRequestFrame} and the {@link CallResponseFrame}; by convention, existing
 * implementations leave arg1 at zero length for {@link CallResponseFrame} frames.
 * <p>
 * The size of arg1 is at most 16KiB.
 */
public class CallResponseFrame extends CallFrame {

    private ResponseCode responseCode;
    private Trace tracing;
    private Map<String, String> headers;

    public CallResponseFrame(long id, byte flags, ResponseCode responseCode, Trace tracing, Map<String, String> headers,
                             ChecksumType checksumType, int checksum, ByteBuf payload) {
        this.id = id;
        this.flags = flags;
        this.responseCode = responseCode;
        this.tracing = tracing;
        this.headers = headers;
        this.checksumType = checksumType;
        this.checksum = checksum;
        this.payload = payload;
    }

    public CallResponseFrame(long id, ResponseCode responseCode, Trace tracing, Map<String, String> headers,
                             ChecksumType checksumType, int checksum) {
        this(id, (byte)0, responseCode, tracing, headers, checksumType, checksum, null);
    }

    protected CallResponseFrame(long id) {
        this(id,(byte)0, null, null, null, null, 0, null);
    }

    public boolean ok() {
        return (this.responseCode == ResponseCode.OK);
    }

    @Override
    public FrameType getType() {
        return FrameType.CallResponse;
    }

    public Trace getTracing() {
        return this.tracing;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
    }

    public ResponseCode getResponseCode() {
        return responseCode;
    }

    @Override
    public ByteBuf encodeHeader(ByteBufAllocator allocator) {
        ByteBuf buffer = allocator.buffer(1024);

        // flags:1
        buffer.writeByte(getFlags());

        // code:1
        buffer.writeByte(getResponseCode().byteValue());

        // tracing:25
        CodecUtils.encodeTrace(getTracing(), buffer);

        // headers -> nh:1 (hk~1 hv~1){nh}
        CodecUtils.encodeSmallHeaders(getHeaders(), buffer);

        // csumtype:1
        buffer.writeByte(getChecksumType().byteValue());

        // (csum:4){0,1}
        CodecUtils.encodeChecksum(getChecksum(), getChecksumType(), buffer);

        return buffer;
    }

    @Override
    public void decode(TFrame tFrame) {

        // flags:1
        flags = tFrame.payload.readByte();

        // code:1
        responseCode = ResponseCode.fromByte(tFrame.payload.readByte());

        // tracing:25
        tracing = CodecUtils.decodeTrace(tFrame.payload);

        // headers -> nh:1 (hk~1, hv~1){nh}
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
        return new CallResponseFrame(this.id, this.flags, this.responseCode,
                this.tracing, this.headers, this.checksumType, this.checksum, payload);
    }
}
