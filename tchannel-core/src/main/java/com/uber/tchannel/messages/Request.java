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

import com.google.common.annotations.VisibleForTesting;
import com.uber.tchannel.frames.FrameType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.RetryFlag;
import com.uber.tchannel.headers.TransportHeaders;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
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
    protected Map<String, String> transportHeaders = new HashMap<>();

    protected final int retryLimit;

    protected Request(Builder builder) {
        this.service = builder.service;
        this.arg1 = builder.arg1;
        this.arg2 = builder.arg2;
        this.arg3 = builder.arg3;
        this.ttl = builder.ttl;
        this.transportHeaders = builder.transportHeaders;
        this.endpoint = builder.endpoint;
        this.retryLimit = builder.retryLimit;
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
        this.retryLimit = 0;
    }

    @Override
    public long getId() {
        return this.id;
    }

    @Override
    public FrameType getType() {
        return type;
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
            arg1 == null ? "null" : arg1.toString(CharsetUtil.UTF_8),
            arg2 == null ? "null" : arg2.toString(CharsetUtil.UTF_8),
            arg3 == null ? "null" : arg3.toString(CharsetUtil.UTF_8)
        );
    }

    @Override
    public void touch() {
        touch(null);
    }

    @Override
    public void touch(Object hint) {
        if (arg2 != null) {
            arg2.touch();
        }
        if (arg2 != null) {
            arg2.touch(hint);
        }
        if (arg3 != null) {
            arg3.touch(hint);
        }
    }

    @Override
    public void release() {
        RuntimeException releaseError = null;
        if (arg1 != null) {
            try {
                arg1.release();
                arg1 = null;
            } catch (RuntimeException ex) {
                releaseError = ex;
            }
        }

        if (arg2 != null) {
            try {
                arg2.release();
                arg2 = null;
            } catch (RuntimeException ex) {
                if (releaseError != null) {
                    releaseError.addSuppressed(ex);
                } else {
                    releaseError = ex;
                }
            }
        }

        if (arg3 != null) {
            try {
                arg3.release();
                arg3 = null;
            } catch (RuntimeException ex) {
                if (releaseError != null) {
                    releaseError.addSuppressed(ex);
                } else {
                    releaseError = ex;
                }
            }
        }

        if (releaseError != null) {
            throw releaseError;
        }
    }

    public void releaseQuietly() {
        try {
            release();
        } catch (RuntimeException rex) {
            //no op
        }
        this.arg1 = null;
        this.arg2 = null;
        this.arg3 = null;
    }

    @Override
    public void close() {
        release();
    }

    public int argSize() {
        int size = 0;

        if (arg1 != null) {
            size += arg1.readableBytes();
        }

        if (arg2 != null) {
            size += arg2.readableBytes();
        }

        if (arg3 != null) {
            size += arg3.readableBytes();
        }

        return size;
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

    public void setTransportHeader(String key, String value) {
        this.transportHeaders.put(key, value);
    }

    public long getTimeout() {
        return ttl;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public void reset() {
        // reset the read index for retries
        arg1.resetReaderIndex();
        arg2.resetReaderIndex();
        arg3.resetReaderIndex();
    }

    public String getEndpoint() {
        if (this.endpoint == null) {
            this.endpoint = this.arg1.toString(CharsetUtil.UTF_8);
        }

        return this.endpoint;
    }

    public ArgScheme getArgScheme() {
        return ArgScheme.toScheme(transportHeaders.get(TransportHeaders.ARG_SCHEME_KEY));
    }

    public void setArgScheme(ArgScheme argScheme) {
        setTransportHeader(TransportHeaders.ARG_SCHEME_KEY, argScheme.getScheme());
    }

    public String getRetryFlags() {
        return transportHeaders.get(TransportHeaders.RETRY_FLAGS_KEY);
    }

    public void setRetryFlags(Set<RetryFlag> flags) {
        setRetryFlags(RetryFlag.flagsToString(flags));
    }

    public void setRetryFlags(String flags) {
        if (!RetryFlag.validFlags(flags)) {
            throw new UnsupportedOperationException("Invalid retry flag: " + flags);
        }

        setTransportHeader(TransportHeaders.RETRY_FLAGS_KEY, flags);
    }

    public static @Nullable Request build(long id, long ttl,
                                String service, Map<String, String> transportHeaders,
                                ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        ArgScheme argScheme = ArgScheme.toScheme(transportHeaders.get(TransportHeaders.ARG_SCHEME_KEY));
        if (argScheme == null) {
            return null;
        }

        return Request.build(argScheme, id, ttl, service, transportHeaders, arg1, arg2, arg3);
    }

    public static @Nullable Request build(ArgScheme argScheme, long id, long ttl,
                                String service, Map<String, String> transportHeaders,
                                ByteBuf arg1, ByteBuf arg2, ByteBuf arg3) {
        Request req;
        switch (argScheme) {
            case RAW:
                req = new RawRequest(id, ttl, service, transportHeaders, arg1, arg2, arg3);
                break;
            case JSON:
                req = new JsonRequest<>(id, ttl, service, transportHeaders, arg1, arg2, arg3);
                break;
            case THRIFT:
                req = new ThriftRequest<>(id, ttl, service, transportHeaders, arg1, arg2, arg3);
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
        private @Nullable String endpoint = null;
        private @Nullable ByteBuf arg1 = null;
        private final String service;

        /*
        * Time To Live in milliseconds. Defaults to a reasonable 100ms, as requests *cannot* be made without a TTL set.
        *
        * see: https://github.com/uber/tchannel/blob/master/docs/protocol.md#ttl4
        */
        private long ttl = 100;

        // Default: initial request + 4 retries
        protected int retryLimit = 4;

        public Builder(String service, String endpoint) {
            this.service = service;
            this.setEndpoint(endpoint);
        }

        public Builder(String service, ByteBuf arg1) {
            this.service = service;
            this.arg1 = arg1;
        }

        private Builder setEndpoint(String endpoint) {
            this.setArg1(Unpooled.wrappedBuffer(endpoint.getBytes(StandardCharsets.UTF_8)));
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

        @VisibleForTesting
        ByteBuf getArg1() {
            return arg1;
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

        public Builder setRetryLimit(int retryLimit) {
            this.retryLimit = retryLimit;
            return this;
        }

        public Builder setTimeout(long timeoutMillis) {
            this.ttl = timeoutMillis;
            return this;
        }

        public Builder setTimeout(long timeout, TimeUnit timeUnit) {
            this.ttl = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
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
                throw new IllegalStateException("`timeout` must be greater than 0.");
            }

            if (retryLimit < 0) {
                throw new IllegalStateException("`retryLimit` must be greater equal to 0.");
            }

            return this;
        }

        public void release() {
            //no need to clear arg1, the places where arg1 is set are:
            // a) Builder(java.lang.String, java.lang.String) uses Unpooled.wrappedBuffer
            // b) Builder.Builder(java.lang.String, io.netty.buffer.ByteBuf) - we haven't allocated ByteBuf, we
            // shouldn't clean it up
            if (arg2 != null) {
                arg2.release();
                arg2 = null;
            }

            if (arg3 != null) {
                arg3.release();
                arg3 = null;
            }
        }

    }

}
