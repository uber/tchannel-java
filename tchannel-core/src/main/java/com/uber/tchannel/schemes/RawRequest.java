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

package com.uber.tchannel.schemes;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a TChannel request message with `raw` arg scheme encoding.
 * <p>
 * All RPC messages over TChannel contain 3 opaque byte payloads, namely, arg{1,2,3}. TChannel makes no assumptions
 * about the contents of these messages. In order to make sense of these arg payloads, TChannel has the notion of
 * `arg schemes` which define standardized schemas and serialization formats over the raw arg{1,2,3} payloads. The
 * supported `arg schemes` are `thrift`, `json`, `http` and `sthrift`. These request / response messages will be built
 * on top of {@link RawRequest} and {@link RawResponse} messages.
 * <p>
 * <h3>From the Docs</h3>
 * The `raw` encoding is intended for any custom encodings you want to do that
 * are not part of TChannel but are application specific.
 */
public final class RawRequest implements RawMessage {

    private final long id;
    private final long ttl;
    private final String service;
    private final Map<String, String> transportHeaders;
    private final ByteBuf arg1;
    private final ByteBuf arg2;
    private final ByteBuf arg3;

    public RawRequest(long id, long ttl, String service, Map<String, String> transportHeaders,
                      ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        this.id = id;
        this.ttl = ttl;
        this.service = service;
        this.transportHeaders = transportHeaders != null ? transportHeaders : new HashMap<String, String>();
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
    }

    @Override
    public long getId() {
        return this.id;
    }

    public long getTTL() {
        return ttl;
    }

    public String getService() {
        return this.service;
    }

    @Override
    public ByteBuf getArg1() {
        return arg1;
    }

    @Override
    public ByteBuf getArg2() {
        return arg2;
    }

    @Override
    public ByteBuf getArg3() {
        return arg3;
    }

    @Override
    public Map<String, String> getTransportHeaders() {
        return this.transportHeaders;
    }

    @Override
    public String toString() {
        return String.format(
                "<%s id=%d service=%s transportHeaders=%s arg1=%s arg2=%s arg3=%s>",
                this.getClass().getSimpleName(),
                this.id,
                this.service,
                this.transportHeaders,
                this.arg1.toString(CharsetUtil.UTF_8),
                this.arg2.toString(CharsetUtil.UTF_8),
                this.arg3.toString(CharsetUtil.UTF_8)
        );
    }
}
