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

import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class RawRequest extends Request {
    private String header = null;
    private String body = null;

    protected RawRequest(Builder builder) {
        super(builder);
        this.body = builder.body;
        this.header = builder.header;
    }

    protected RawRequest(long id, long ttl,
                      String service, Map<String, String> transportHeaders,
                      ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        super(id, ttl, service, transportHeaders, arg1, arg2, arg3);
    }

    public String getHeader() {
        if (this.header == null) {
            if (arg2 == null) {
                this.header = "";
            } else {
                this.header = this.arg2.toString(CharsetUtil.UTF_8);
            }
        }

        return this.header;
    }

    public String getBody() {
        if (this.body == null) {
            if (arg3 == null) {
                this.body = "";
            } else {
                this.body = this.arg3.toString(CharsetUtil.UTF_8);
            }
        }

        return this.body;
    }

    public static class Builder extends Request.Builder {
        protected String header = null;
        protected String body = null;

        public Builder(String service, String endpoint) {
            super(service, endpoint);
            this.transportHeaders.put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
        }

        public Builder(String service, ByteBuf arg1) {
            super(service, arg1);
            this.transportHeaders.put(TransportHeaders.ARG_SCHEME_KEY, ArgScheme.RAW.getScheme());
        }

        @Override
        public Builder setTimeout(long timeoutMillis) {
            super.setTimeout(timeoutMillis);
            return this;
        }

        @Override
        public Builder setTimeout(long timeout, TimeUnit timeUnit) {
            super.setTimeout(timeout, timeUnit);
            return this;
        }

        @Override
        public Builder setId(long id) {
            super.setId(id);
            return this;
        }

        @Override
        public Builder setRetryLimit(int retryLimit) {
            super.setRetryLimit(retryLimit);
            return this;
        }

        @Override
        public Builder setArg2(ByteBuf arg2) {
            super.setArg2(arg2);
            this.header = null;
            return this;
        }

        @Override
        public Builder setArg3(ByteBuf arg3) {
            super.setArg3(arg3);
            this.body = null;
            return this;
        }

        @Override
        public Builder setTransportHeader(String key, String value) {
            super.setTransportHeader(key, value);
            return this;
        }

        @Override
        public Builder setTransportHeaders(Map<String, String> transportHeaders) {
            super.setTransportHeaders(transportHeaders);
            return this;
        }

        public final Builder setHeader(String header) {
            this.setArg2(Unpooled.wrappedBuffer(header.getBytes()));
            this.header = header;
            return this;
        }

        public final Builder setBody(String body) {
            setArg3(Unpooled.wrappedBuffer(((String) body).getBytes()));
            this.body = body;
            return this;
        }

        @Override
        public Builder validate() {
            super.validate();

            if (arg2 == null) {
                arg2 = TChannelUtilities.emptyByteBuf;
            }

            if (arg3 == null) {
                arg3 = TChannelUtilities.emptyByteBuf;
            }

            return this;
        }

        public RawRequest build() {
            return new RawRequest(this.validate());
        }
    }
}
