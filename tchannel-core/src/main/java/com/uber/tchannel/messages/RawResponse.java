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

import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.util.Map;

/**
 * Represents a TChannel response message with `raw` arg scheme encoding.
 * <p>
 * All RPC frames over TChannel contain 3 opaque byte payloads, namely, arg{1,2,3}. TChannel makes no assumptions
 * about the contents of these frames. In order to make sense of these arg payloads, TChannel has the notion of
 * `arg messages` which define standardized schemas and serialization formats over the raw arg{1,2,3} payloads. The
 * supported `arg messages` are `thrift`, `json`, `http` and `sthrift`. These request / response frames will be built
 * on top of {@link RawRequest} and {@link RawResponse} frames.
 * <p>
 * <h3>From the Docs</h3>
 * The `raw` encoding is intended for any custom encodings you want to do that
 * are not part of TChannel but are application specific.
 */
public class RawResponse extends Response implements RawMessage {

    private String header = null;
    private String body = null;

    private RawResponse(Builder builder) {
        super(builder);
    }

    protected RawResponse(long id, ResponseCode responseCode,
                          Map<String, String> transportHeaders,
                          ByteBuf arg2, ByteBuf arg3) {
        super(id, responseCode, transportHeaders, arg2, arg3);
    }

    protected RawResponse(ErrorResponse error) {
        super(error);
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

    @Override
    public String toString() {
        if (isError()) {
            return getError().toString();
        }

        return String.format(
            "<%s responseCode=%s transportHeaders=%s header=%s body=%s>",
            this.getClass().getSimpleName(),
            this.responseCode,
            this.transportHeaders,
            this.getHeader(),
            this.getBody()
        );
    }

    public static class Builder extends Response.Builder {

        protected String header = null;
        protected String body = null;

        public Builder(Request req) {
            super(req);
        }

        public Builder setResponseCode(ResponseCode responseCode) {
            this.responseCode = responseCode;
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

        public RawResponse build() {
            return new RawResponse(this.validate());
        }
    }
}
