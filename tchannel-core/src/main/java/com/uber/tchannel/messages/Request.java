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

import com.uber.tchannel.frames.FrameType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class Request implements RawMessage {

    protected final FrameType type = FrameType.CallRequest;

    protected long id = -1;
    protected String service = null;
    protected ByteBuf arg1 = null;
    protected ByteBuf arg2 = null;
    protected ByteBuf arg3 = null;

    protected String endpoint = null;
    protected long ttl = 100;
    protected Map<String, String> transportHeaders = new HashMap<String, String>();

    protected Request(Builder builder) {
        this.service = builder.service;
        this.arg1 = builder.arg1;
        this.arg2 = builder.arg2;
        this.arg3 = builder.arg3;
        this.ttl = builder.ttl;
        this.transportHeaders = builder.transportHeaders;
        this.endpoint = builder.endpoint;
    }

    protected Request(long id, long ttl,
                      String service, Map<String, String> transportHeaders,
                      ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        this.id = id;
        this.ttl = ttl;
        this.service = service;
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
        this.transportHeaders = transportHeaders;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public FrameType getType() {
        return type;
    }

    public void setId(int id) {
        this.id = id;
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

    public final void appendArg2(ByteBuf arg) {
        arg2 = Unpooled.wrappedBuffer(arg2, arg);
    }

    public final void appendArg3(ByteBuf arg) {
        arg3 = Unpooled.wrappedBuffer(arg3, arg);
    }

    public final void setTransportHeader(String key, String value) {
        this.transportHeaders.put(key, value);
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

    public void release() {
        arg1.release();
        arg2.release();
        arg3.release();
        arg1 = null;
        arg2 = null;
        arg3 = null;
    }

    public String getEndpoint() {
        if (this.endpoint == null) {
            this.endpoint = this.arg1.toString(CharsetUtil.UTF_8);
        }

        return this.endpoint;
    }

    public final ArgScheme getArgScheme() {
        return ArgScheme.toScheme(transportHeaders.get(TransportHeaders.ARG_SCHEME_KEY));
    }

    public static Request build(long id, long ttl,
                                String service, Map<String, String> transportHeaders,
                                ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        ArgScheme argScheme = ArgScheme.toScheme(transportHeaders.get(TransportHeaders.ARG_SCHEME_KEY));
        if (argScheme == null) {
            return null;
        }

        return Request.build(argScheme, id, ttl, service, transportHeaders, arg1, arg2, arg3);
    }

    public static Request build(ArgScheme argScheme, long id, long ttl,
                                String service, Map<String, String> transportHeaders,
                                ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        Request req;
        switch (argScheme) {
            case RAW:
                req = new RawRequest(id, ttl, service, transportHeaders, arg1, arg2, arg3);
                break;
            case JSON:
                req = new JsonRequest(id, ttl, service, transportHeaders, arg1, arg2, arg3);
                break;
            case THRIFT:
                req = new ThriftRequest(id, ttl, service, transportHeaders, arg1, arg2, arg3);
                break;
            default:
                req = null;
                break;
        }

        return req;
    }


    public static class Builder {

        protected Map<String, String> transportHeaders = new HashMap<>();
        protected ByteBuf arg2 = null;
        protected ByteBuf arg3 = null;

        private long id = -1;
        private String endpoint = null;
        private ByteBuf arg1 = null;
        private final String service;

        /*
        * Time To Live in milliseconds. Defaults to a reasonable 100ms, as requests *cannot* be made without a TTL set.
        *
        * see: https://github.com/uber/tchannel/blob/master/docs/protocol.md#ttl4
        */
        private long ttl = 100;

        public Builder(String service, String endpoint) {
            this.service = service;
            this.setEndpoint(endpoint);
        }

        public Builder(String service, ByteBuf arg1) {
            this.service = service;
            this.arg1 = arg1;
        }

        private Builder setEndpoint(String endpoint) {
            this.setArg1(Unpooled.wrappedBuffer(endpoint.getBytes()));
            this.endpoint = endpoint;

            return this;
        }

        private Builder setArg1(ByteBuf arg1) {
            if (this.arg1 != null) {
                this.arg1.release();
            }

            this.arg1 = arg1;
            this.endpoint = null;
            return this;
        }

        public Builder setArg2(ByteBuf arg2) {
            if (this.arg2 != null) {
                this.arg2.release();
            }

            this.arg2 = arg2;
            return this;
        }

        public Builder setArg3(ByteBuf arg3) {
            if (this.arg3 != null) {
                this.arg3.release();
            }

            this.arg3 = arg3;
            return this;
        }

        public Builder setTransportHeader(String key, String value) {
            this.transportHeaders.put(key, value);
            return this;
        }

        public Builder setTransportHeaders(Map<String, String> transportHeaders) {
            this.transportHeaders = transportHeaders;
            return this;
        }

        public Builder setId(long id) {
            this.id = id;
            return this;
        }

        /**
         * @param ttl TTL in milliseconds
         * @return Builder
         */
        public Builder setTTL(long ttl) {
            this.ttl = ttl;
            return this;
        }

        /**
         * @param ttl      TTL in `timeUnit` units
         * @param timeUnit time unit for the `ttl`
         * @return Builder
         */
        public Builder setTTL(long ttl, TimeUnit timeUnit) {
            this.ttl = TimeUnit.MILLISECONDS.convert(ttl, timeUnit);
            return this;
        }

        public Builder validate() {
            if (service == null) {
                throw new IllegalStateException("`service` cannot be null.");
            }

            if (arg1 == null && endpoint == null) {
                throw new IllegalStateException("`arg1` or `endpoint` cannot be null.");
            }

            if (ttl <= 0) {
                throw new IllegalStateException("`ttl` must be greater than 0.");
            }

            return this;
        }
    }
}
