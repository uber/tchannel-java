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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.Map;

/**
 * This must be the first message sent on a new connection. It is used to negotiate a common protocol version
 * and describe the service names on both ends. In the future, we will likely use this to negotiate authentication
 * and authorization between services.
 */
public final class InitRequestFrame extends InitFrame {

    private int version;
    private Map<String, String> headers;

    public InitRequestFrame(long id, int version, Map<String, String> headers) {
        this.id = id;
        this.version = version;
        this.headers = headers;
    }

    protected InitRequestFrame(long id) {
        this.id = id;
    }

    public int getVersion() {
        return this.version;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
    }

    public FrameType getType() {
        return FrameType.InitRequest;
    }

    public String getHostPort() {
        return this.headers.get(HOST_PORT_KEY);
    }

    public void setHostPort(String hostPort) {
        this.headers.put(HOST_PORT_KEY, hostPort);
    }

    public String getProcessName() {
        return this.headers.get(PROCESS_NAME_KEY);
    }

    public void setProcessName(String processName) {
        this.headers.put(PROCESS_NAME_KEY, processName);
    }

    @Override
    public String toString() {
        return String.format("<%s id=%d version=%d headers=%s>",
                this.getClass().getSimpleName(),
                this.id,
                this.version,
                this.headers
        );
    }

    @Override
    public ByteBuf encodeHeader(ByteBufAllocator allocator) {
        // Allocate new ByteBuf
        ByteBuf buffer = allocator.buffer(256);

        // version:2
        buffer.writeShort(getVersion());

        // headers -> nh:2 (key~2 value~2){nh}
        CodecUtils.encodeHeaders(getHeaders(), buffer);

        return buffer;
    }

    @Override
    public void decode(TFrame tFrame) {
        // version:2
        version = tFrame.payload.readUnsignedShort();

        // headers -> nh:2 (key~2 value~2){nh}
        headers = CodecUtils.decodeHeaders(tFrame.payload);
    }
}
