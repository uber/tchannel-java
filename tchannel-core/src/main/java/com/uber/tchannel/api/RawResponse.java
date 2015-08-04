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

package com.uber.tchannel.api;

import com.uber.tchannel.messages.FullMessage;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.util.Map;

public class RawResponse implements Response<ByteBuf>, FullMessage {

    private final long id;
    private final Map<String, String> headers;
    private final ByteBuf arg1;
    private final ByteBuf arg2;
    private final ByteBuf arg3;

    public RawResponse(long id, Map<String, String> headers, ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        this.id = id;
        this.headers = headers;
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
    }

    public long getId() {
        return this.id;
    }

    public Map<String, String> getHeaders() {
        return this.headers;
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

    @Override
    public String toString() {
        return String.format(
                "<%s id=%d headers=%s arg1=%s arg2=%s arg3=%s>",
                this.getClass().getSimpleName(),
                this.id,
                this.headers,
                this.arg1.toString(CharsetUtil.UTF_8),
                this.arg2.toString(CharsetUtil.UTF_8),
                this.arg3.toString(CharsetUtil.UTF_8)
        );
    }
}
