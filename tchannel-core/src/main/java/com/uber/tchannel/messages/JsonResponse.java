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
import com.uber.tchannel.headers.ArgScheme;
import io.netty.buffer.ByteBuf;

import java.util.Map;

public final class JsonResponse<T> extends EncodedResponse<T> {

    private JsonResponse(Builder<T> builder) {
        super(builder);
    }

    protected JsonResponse(long id, ResponseCode responseCode,
                              Map<String, String> transportHeaders,
                              ByteBuf arg2, ByteBuf arg3) {
        super(id, responseCode, transportHeaders, arg2, arg3);
    }

    protected JsonResponse(ErrorResponse error) {
        super(error);
    }

    public static class Builder<T> extends EncodedResponse.Builder<T> {

        public Builder(JsonRequest req) {
            super(req);
            this.argScheme = ArgScheme.JSON;
        }

        @Override
        public Builder<T> validate() {
            super.validate();
            return this;
        }

        public JsonResponse<T> build() {
            return new JsonResponse(this.validate());
        }

        @Override
        public Builder<T> setArg2(ByteBuf arg2) {
            super.setArg2(arg2);
            return this;
        }

        @Override
        public Builder<T> setArg3(ByteBuf arg3) {
            super.setArg3(arg3);
            return this;
        }

        @Override
        public Builder<T> setHeader(String key, String value) {
            super.setHeader(key, value);
            return this;
        }

        @Override
        public Builder<T> setHeaders(Map<String, String> headers) {
            super.setHeaders(headers);
            return this;
        }

        @Override
        public Builder<T> setBody(T body) {
            super.setBody(body);
            return this;
        }

        @Override
        public Builder<T> setTransportHeader(String key, String value) {
            super.setTransportHeader(key, value);
            return this;
        }

        @Override
        public Builder<T> setTransportHeaders(Map<String, String> transportHeaders) {
            super.setTransportHeaders(transportHeaders);
            return this;
        }

        @Override
        public Builder<T> setResponseCode(ResponseCode responseCode) {
            super.setResponseCode(responseCode);
            return this;
        }
    }
}
