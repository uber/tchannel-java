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

import com.google.common.collect.ImmutableMap;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.tracing.Trace;
import com.uber.tchannel.tracing.TraceableRequest;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

public abstract class EncodedRequest<T> extends Request implements TraceableRequest {

    private static final ImmutableMap<ArgScheme, Serializer.SerializerInterface> DEFAULT_SERIALIZERS =
            ImmutableMap.of(ArgScheme.JSON, new JSONSerializer(), ArgScheme.THRIFT, new ThriftSerializer());

    private static final Serializer serializer = new Serializer(DEFAULT_SERIALIZERS);

    protected Map<String, String> headers;
    protected T body = null;

    protected EncodedRequest(Builder<T> builder) {
        super(builder);
        this.body = builder.body;
        this.headers = builder.headers;
    }

    protected EncodedRequest(long id, long ttl, Trace trace,
                         String service, Map<String, String> transportHeaders,
                         ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        super(id, ttl, trace, service, transportHeaders, arg1, arg2, arg3);
    }

    @Override
    public Map<String, String> getHeaders() {
        if (headers == null) {
            if (arg2 != null) {
                headers = serializer.decodeHeaders(this);
            } else {
                return new HashMap<>();
            }
        }

        return headers;
    }

    public String getHeader(String key) {
        return getHeaders().get(key);
    }

    @Override
    public void setHeaders(Map<String, String> headers) {
        if (arg2 != null) {
            arg2.release();
        }
        arg2 = serializer.encodeHeaders(headers, getArgScheme());
        this.headers = null;
    }

    public T getBody(Class<T> bodyType) {
        if (body == null) {
            if (arg3 != null) {
                body = serializer.decodeBody(this, bodyType);
            } else {
                return null;
            }
        }

        return body;
    }

    /**
     * @param <T> request body type
     */
    public static class Builder<T> extends Request.Builder {

        protected Map<String, String> headers = new HashMap<>();
        protected T body = null;
        protected ArgScheme argScheme;

        public Builder(String service, String endpoint) {
            super(service, endpoint);
        }

        public Builder(String service, ByteBuf arg1) {
            super(service, arg1);
        }

        @Override
        public Builder<T> setArg2(ByteBuf arg2) {
            if (arg2 != null && !this.headers.isEmpty()) {
                throw new IllegalStateException("Cannot set both `arg2` and `headers`.");
            }

            super.setArg2(arg2);
            return this;
        }

        @Override
        public Builder<T> setArg3(ByteBuf arg3) {
            if (arg3 != null && this.body != null) {
                throw new IllegalStateException("Cannot set both `arg3` and `body`.");
            }

            super.setArg3(arg3);
            return this;
        }

        public Builder<T> setHeader(String key, String value) {
            this.headers.put(key, value);
            return this;
        }

        public Builder<T> setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder<T> setBody(T body) {
            this.body = body;
            return this;
        }

        private Builder<T> validateHeader() {
            if (arg2 != null) {
                return this;
            }

            arg2 = serializer.encodeHeaders(this.headers, argScheme);

            return this;
        }

        private Builder<T> validateBody() {
            if (arg3 != null) {
                return this;
            }

            if (body == null) {
                arg3 = TChannelUtilities.emptyByteBuf;
            } else {
                arg3 = serializer.encodeBody(this.body, argScheme);
            }

            return this;
        }

        @Override
        public Builder<T> validate() {
            super.validate();
            this.validateHeader();
            this.validateBody();
            return this;
        }

    }

}
