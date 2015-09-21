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

package com.uber.tchannel.api.handlers;

import com.google.common.reflect.TypeToken;
import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.schemes.JSONSerializer;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;

import java.util.Map;

public abstract class JSONRequestHandler<T, U> implements RequestHandler {

    private TypeToken<T> requestType = new TypeToken<T>(getClass()) { };
    private TypeToken<U> responseType = new TypeToken<U>(getClass()) { };

    private JSONSerializer serializer = new JSONSerializer();

    @Override
    public RawResponse handle(RawRequest request) {

        String endpoint = serializer.decodeEndpoint(request.getArg1());
        Map<String, String> headers = serializer.decodeHeaders(request.getArg2());
        T param = serializer.decodeBody(request.getArg3(), this.getRequestType());

        U response = handleImpl(param);

        RawResponse rawResponse = new RawResponse(
                request.getId(),
                ResponseCode.OK,
                request.getTransportHeaders(),
                serializer.encodeEndpoint(endpoint),
                serializer.encodeHeaders(headers),
                serializer.encodeBody(response)
        );

        return rawResponse;
    }

    public Class<T> getRequestType() {
        return (Class<T>) requestType.getRawType();
    }

    public Class<U> getResponseType() {
        return (Class<U>) responseType.getRawType();
    }

    public abstract U handleImpl(T params);
}
