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

import java.util.Map;

public final class Request<T> {

    private final String endpoint;
    private final Map<String, String> headers;
    private final T body;

    private Request(Builder<T> builder) {
        this.endpoint = builder.endpoint;
        this.headers = builder.headers;
        this.body = builder.body;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public T getBody() {
        return body;
    }

    @Override
    public String toString() {
        return String.format(
                "<%s endpoint=%s headers=%s body=%s>",
                this.getClass().getSimpleName(),
                this.endpoint,
                this.headers,
                this.body
        );
    }

    public static class Builder<U> {

        private String endpoint;
        private Map<String, String> headers;
        private U body;

        public Builder(U body) {
            this.body = body;
        }

        public Builder<U> setEndpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder<U> setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Request<U> build() {
            return new Request<>(this);
        }

    }
}
