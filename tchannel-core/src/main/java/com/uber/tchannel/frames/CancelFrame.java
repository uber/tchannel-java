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

import com.uber.tchannel.codecs.CodecUtils;
import com.uber.tchannel.codecs.TFrame;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public final class CancelFrame extends Frame {

    private long ttl;
    private Trace tracing;
    private String why;

    /**
     * Designated Constructor
     *
     * @param id      unique id of the message
     * @param ttl     ttl on the wire
     * @param tracing tracing information
     * @param why     why the message was canceled
     */
    public CancelFrame(long id, long ttl, Trace tracing, String why) {
        this.id = id;
        this.ttl = ttl;
        this.tracing = tracing;
        this.why = why;
    }

    protected CancelFrame(long id) {
        this.id = id;
    }

    public long getTTL() {
        return ttl;
    }

    public Trace getTracing() {
        return tracing;
    }

    @Override
    public FrameType getType() {
        return FrameType.Cancel;
    }

    @Override
    public ByteBuf encodeHeader(ByteBufAllocator allocator) {
        ByteBuf buffer = allocator.buffer(31);

        // ttl:4
        buffer.writeInt((int) getTTL());

        // tracing:25
        CodecUtils.encodeTrace(getTracing(), buffer);

        // why~2
        CodecUtils.encodeString(getWhy(), buffer);

        return buffer;
    }

    @Override
    public void decode(TFrame tFrame) {
        // ttl:4
        ttl = tFrame.payload.readUnsignedInt();

        // tracing:25
        tracing = CodecUtils.decodeTrace(tFrame.payload);

        // why~2
        why = CodecUtils.decodeString(tFrame.payload);

        tFrame.release();
    }

    public String getWhy() {
        return why;
    }

}
