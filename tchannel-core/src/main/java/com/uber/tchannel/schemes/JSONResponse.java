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

import com.uber.tchannel.api.Response;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;

import java.util.Map;

public class JSONResponse<T> implements Response<String, Map<String, String>, T> {

    private final long id;
    private final Map<String, String> transportHeaders;
    private final String method;
    private final Map<String, String> applicationHeaders;
    private final T body;

    public JSONResponse(long id, Map<String, String> transportHeaders, String method,
                        Map<String, String> applicationHeaders, T body) {
        this.id = id;
        this.transportHeaders = transportHeaders;
        this.transportHeaders.putIfAbsent(
                TransportHeaders.ARG_SCHEME_KEY,
                ArgScheme.JSON.getScheme()
        );
        this.method = method;
        this.applicationHeaders = applicationHeaders;
        this.body = body;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public Map<String, String> getTransportHeaders() {
        return this.transportHeaders;
    }

    @Override
    public String getMethod() {
        return this.method;
    }

    @Override
    public Map<String, String> getApplicationHeaders() {
        return this.applicationHeaders;
    }

    @Override
    public T getBody() {
        return this.body;
    }

    @Override
    public String toString() {
        return String.format(
                "<%s id=%d transportHeaders=%s method=%s applicationHeaders=%s body=%s>",
                this.getClass().getSimpleName(),
                this.id,
                this.transportHeaders,
                this.getMethod(),
                this.getApplicationHeaders(),
                this.getBody()
        );
    }

}
