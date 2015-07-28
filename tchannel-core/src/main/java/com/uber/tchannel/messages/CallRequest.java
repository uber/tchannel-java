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
public class CallRequest extends AbstractCallMessage {

    private final long ttl;
    private final Trace tracing;
    private final String service;
    private final Map<String, String> headers;

    public CallRequest(long id, byte flags, long ttl, Trace tracing, String service, Map<String, String> headers,
                       byte checksumType, int checksum, ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {

        super(id, MessageType.CallRequest, flags, checksumType, checksum, arg1, arg2, arg3);
        this.ttl = ttl;
        this.service = service;
        this.tracing = tracing;
        this.headers = headers;
    }

    public long getTtl() {
        return ttl;
    }

    public Trace getTracing() {
        return tracing;
    }

    public String getService() {
        return service;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }
}
