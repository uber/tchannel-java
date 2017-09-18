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
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.frames.FrameType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a TChannel response message with `raw` arg scheme encoding.
 * <p>
 * All RPC frames over TChannel contain 3 opaque byte payloads, namely, arg{1,2,3}. TChannel makes no assumptions
 * about the contents of these frames. In order to make sense of these arg payloads, TChannel has the notion of
 * `arg messages` which define standardized schemas and serialization formats over the raw arg{1,2,3} payloads. The
 * supported `arg messages` are `thrift`, `json`, `http` and `sthrift`. These request / response frames will be built
 * on top of {@link RawRequest} and {@link Response} frames.
 * <p>
 * <h3>From the Docs</h3>
 * The `raw` encoding is intended for any custom encodings you want to do that
 * are not part of TChannel but are application specific.
 */
public abstract class Response extends ResponseMessage implements RawMessage {

    protected ByteBuf arg2;
    protected ByteBuf arg3;

    private static final ByteBuf arg1 = TChannelUtilities.emptyByteBuf;

    protected long id;
    protected final ResponseCode responseCode;
    protected final Map<String, String> transportHeaders;

    private final ErrorResponse error;

    protected Response(long id, ResponseCode responseCode, Map<String, String> transportHeaders,
                       ByteBuf arg2, ByteBuf arg3) {
        this.id = id;
        this.responseCode = responseCode;
        this.transportHeaders = transportHeaders;
        this.arg2 = arg2;
        this.arg3 = arg3;
        this.type = FrameType.CallResponse;
        this.error = null;
    }

    protected Response(Builder builder) {
        this.id = builder.id;
        this.responseCode = builder.responseCode;
        this.transportHeaders = builder.transportHeaders;
        this.arg2 = builder.arg2;
        this.arg3 = builder.arg3;
        this.type = FrameType.CallResponse;
        this.error = null;
    }

    protected Response(ErrorResponse error) {
        this.error = error;
        this.id = error.getId();
        this.responseCode = null;
        this.transportHeaders = null;
        this.arg2 = null;
        this.arg3 = null;
        this.type = FrameType.Error;
    }

    @Override
    public long getId() {
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public ResponseCode getResponseCode() {
        return responseCode;
    }

    @Override
    public Map<String, String> getTransportHeaders() {
        return this.transportHeaders;
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
    public String toString() {
        return String.format(
            "<%s id=%d transportHeaders=%s arg1=%s arg2=%s arg3=%s>",
            this.getClass().getSimpleName(),
            this.id,
            this.transportHeaders,
            arg1.toString(CharsetUtil.UTF_8),
            this.arg2.toString(CharsetUtil.UTF_8),
            this.arg3.toString(CharsetUtil.UTF_8)
        );
    }

    @Override
    public void release() {
        arg1.release();

        if (arg2 != null) {
            arg2.release();
            arg2 = null;
        }

        if (arg3 != null) {
            arg3.release();
            arg3 = null;
        }
    }

    @Override
    public boolean isError() {
        return type == FrameType.Error || getError() != null;
    }

    public ErrorResponse getError() {
        return this.error;
    }

    public ArgScheme getArgScheme() {
        return ArgScheme.toScheme(transportHeaders.get(TransportHeaders.ARG_SCHEME_KEY));
    }

    public int argSize() {
        int size = 0;

        if (arg2 != null) {
            size += arg2.readableBytes();
        }

        if (arg3 != null) {
            size += arg3.readableBytes();
        }

        return size;
    }

    public static Response build(long id, ResponseCode responseCode,
                                 Map<String, String> transportHeaders,
                                 ByteBuf arg2, ByteBuf arg3) {
        ArgScheme argScheme = ArgScheme.toScheme(transportHeaders.get(TransportHeaders.ARG_SCHEME_KEY));
        if (argScheme == null) {
            return null;
        }

        return Response.build(argScheme, id, responseCode, transportHeaders, arg2, arg3);
    }

    public static Response build(ArgScheme argScheme, long id,
                                ResponseCode responseCode, Map<String, String> transportHeaders,
                                ByteBuf arg2, ByteBuf arg3) {
        Response res;
        switch (argScheme) {
            case RAW:
                res = new RawResponse(id, responseCode, transportHeaders, arg2, arg3);
                break;
            case JSON:
                res = new JsonResponse(id, responseCode, transportHeaders, arg2, arg3);
                break;
            case THRIFT:
                res = new ThriftResponse(id, responseCode, transportHeaders, arg2, arg3);
                break;
            default:
                res = null;
                break;
        }

        return res;
    }

    public static Response build(ArgScheme argScheme, long id, ErrorType errorType, String message) {
        ErrorResponse errorResponse = new ErrorResponse(id, errorType, message);
        return build(argScheme, errorResponse);
    }

    public static Response build(ArgScheme argScheme, ErrorResponse errorResponse) {
        Response res;
        switch (argScheme) {
            case RAW:
                res = new RawResponse(errorResponse);
                break;
            case JSON:
                res = new JsonResponse(errorResponse);
                break;
            case THRIFT:
                res = new ThriftResponse(errorResponse);
                break;
            default:
                res = new RawResponse(errorResponse);
                break;
        }

        return res;
    }

    public static class Builder {

        protected Map<String, String> transportHeaders = new HashMap<>();
        protected ByteBuf arg2 = null;
        protected ByteBuf arg3 = null;
        protected ResponseCode responseCode = ResponseCode.OK;

        private long id = -1;

        public Builder(Request req) {
            this.id = req.getId();
            this.transportHeaders.put(
                TransportHeaders.ARG_SCHEME_KEY,
                req.getTransportHeaders().get(TransportHeaders.ARG_SCHEME_KEY));
        }

        public Builder setResponseCode(ResponseCode responseCode) {
            this.responseCode = responseCode;
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

        public Builder validate() {
            return this;
        }
    }
}
