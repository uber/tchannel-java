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
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public final class ThriftResponse<T> extends EncodedResponse<T> {

    private ThriftResponse(Builder<T> builder) {
        super(builder);
    }

    protected ThriftResponse(long id, ResponseCode responseCode,
                              Map<String, String> transportHeaders,
                              ByteBuf arg2, ByteBuf arg3) {
        super(id, responseCode, transportHeaders, arg2, arg3);
    }

    protected ThriftResponse(ErrorResponse error) {
        super(error);
    }

    /**
     * @param <T> response body type
     */
    public static class Builder<T> extends EncodedResponse.Builder<T> {

        public Builder(@NotNull ThriftRequest<?> req) {
            super(req);
            this.argScheme = ArgScheme.THRIFT;
        }

        /**
         * Validates payload and populates {@link #arg1}, {@link #arg2}, {@link #arg3}.
         *
         * Args <b>>need</b> to be cleared if validation fails.
         *
         * Use {@link #release()} to clear args above.
         *
         * @throws Exception
         *     if validation fails.
         */
        @Override
        public @NotNull Builder<T> validate() {
            super.validate();
            return this;
        }

        /**
         * Validates payload, populates {@link #arg1}, {@link #arg2}, {@link #arg3} and builds {@link ThriftResponse}.
         *
         * Args above will be auto-released if validation fails.
         *
         * Note: Don't call it again if fails.
         * <br/>unless header/body are re-populated this method will fail if called after the initial failure.
         */
        public @NotNull ThriftResponse<T> build() {
            ThriftResponse<T> result;
            boolean release = true;
            try {
                ThriftResponse.Builder<T> validated = this.validate();
                result = new ThriftResponse<>(validated);
                release = false;
            } finally {
                if (release) {
                    this.release();
                }
            }
            return result;
        }

        @Override
        public @NotNull Builder<T> setArg2(ByteBuf arg2) {
            super.setArg2(arg2);
            return this;
        }

        @Override
        public @NotNull Builder<T> setArg3(ByteBuf arg3) {
            super.setArg3(arg3);
            return this;
        }

        @Override
        public @NotNull Builder<T> setHeader(String key, String value) {
            super.setHeader(key, value);
            return this;
        }

        @Override
        public @NotNull Builder<T> setHeaders(Map<String, String> headers) {
            super.setHeaders(headers);
            return this;
        }

        @Override
        public @NotNull Builder<T> setBody(T body) {
            super.setBody(body);
            return this;
        }

        @Override
        public @NotNull Builder<T> setTransportHeader(String key, String value) {
            super.setTransportHeader(key, value);
            return this;
        }

        @Override
        public @NotNull Builder<T> setTransportHeaders(@NotNull Map<String, String> transportHeaders) {
            super.setTransportHeaders(transportHeaders);
            return this;
        }

        @Override
        public @NotNull Builder<T> setResponseCode(ResponseCode responseCode) {
            super.setResponseCode(responseCode);
            return this;
        }
    }
}
