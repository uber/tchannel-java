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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class Request<T> {

    private final String service;
    private final long ttl;
    private final Map<String, String> transportHeaders;
    private final String endpoint;
    private final Map<String, String> headers;
    private final T body;

    private Request(Builder<T> builder) {
        this.service = builder.service;
        this.ttl = builder.ttl;
        this.transportHeaders = builder.transportHeaders;
        this.endpoint = builder.endpoint;
        this.headers = builder.headers;
        this.body = builder.body;
    }

    public String getService() {
        return service;
    }

    public long getTTL() {
        return ttl;
    }

    public Map<String, String> getTransportHeaders() {
        return transportHeaders;
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
                "<%s service=%s transportHeaders=%s endpoint=%s headers=%s body=%s>",
                this.getClass().getSimpleName(),
                this.service,
                this.transportHeaders,
                this.endpoint,
                this.headers,
                this.body
        );
    }

    public static class Builder<U> {

        private String service;
        /*
        * Time To Live in milliseconds. Defaults to a reasonable 100ms, as requests *cannot* be made without a TTL set.
        *
        * see: https://github.com/uber/tchannel/blob/master/docs/protocol.md#ttl4
        */
        private long ttl = 100;
        private Map<String, String> transportHeaders = new HashMap<>();
        private String endpoint;
        private Map<String, String> headers = new HashMap<>();
        private U body;

        public Builder(U body, String service, String endpoint) {
            this.body = body;
            this.service = service;
            this.endpoint = endpoint;
        }

        /**
         * @param ttl TTL in milliseconds
         * @return Builder
         */
        public Builder<U> setTTL(long ttl) {
            this.ttl = ttl;
            return this;
        }

        /**
         * @param ttl      TTL in `timeUnit` units
         * @param timeUnit time unit for the `ttl`
         * @return Builder
         */
        public Builder<U> setTTL(long ttl, TimeUnit timeUnit) {
            this.ttl = TimeUnit.MILLISECONDS.convert(ttl, timeUnit);
            return this;
        }

        public Builder<U> setTransportHeader(String key, String value) {
            this.transportHeaders.put(key, value);
            return this;
        }

        public Builder<U> setTransportHeaders(Map<String, String> transportHeaders) {
            this.transportHeaders.putAll(transportHeaders);
            return this;
        }

        public Builder<U> setHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        public Builder<U> setHeaders(Map<String, String> headers) {
            this.headers.putAll(headers);
            return this;
        }

        public Builder<U> validate() {
            if (service == null) {
                throw new IllegalStateException("`service` cannot be null.");
            }

            if (endpoint == null) {
                throw new IllegalStateException("`endpoint` cannot be null.");
            }

            if (ttl <= 0) {
                throw new IllegalStateException("`ttl` must be greater than 0.");
            }

            return this;
        }

        public Request<U> build() {
            return new Request<>(this.validate());
        }

    }
}
