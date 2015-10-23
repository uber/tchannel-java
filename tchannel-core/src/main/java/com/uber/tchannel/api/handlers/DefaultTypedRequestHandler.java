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
import com.uber.tchannel.api.Request;
import com.uber.tchannel.api.Response;
import com.uber.tchannel.schemes.RawRequest;
import com.uber.tchannel.schemes.RawResponse;
import com.uber.tchannel.schemes.Serializer;

import java.util.Map;
import java.util.ServiceConfigurationError;

public abstract class DefaultTypedRequestHandler<T, U, V extends Serializer.SerializerInterface>
        implements RequestHandler {

    private TypeToken<T> requestType = new TypeToken<T>(getClass()) { };
    private TypeToken<U> responseType = new TypeToken<U>(getClass()) { };
    private TypeToken<V> serializerType = new TypeToken<V>(getClass()) { };

    private V serializer = getSerializerInstance();

    @Override
    public RawResponse handle(RawRequest request) {

        Request<T> requestT = new Request.Builder<>(
                serializer.decodeBody(request.getArg3(), this.getRequestType()),
                request.getService(),
                serializer.decodeEndpoint(request.getArg1()))
                .setHeaders(serializer.decodeHeaders(request.getArg2()))
                .setTransportHeaders(request.getTransportHeaders())
                .setTTL(request.getTTL())
                .build();

        Response<U> responseU = handleImpl(requestT);

        Map<String, String> transportHeaders = responseU.getTransportHeaders();
        transportHeaders = transportHeaders.isEmpty() ? requestT.getTransportHeaders() : transportHeaders;

        RawResponse response = new RawResponse(
                request.getId(),
                responseU.getResponseCode(),
                transportHeaders,
                serializer.encodeHeaders(responseU.getHeaders()),
                serializer.encodeBody(responseU.getBody())
        );

        return response;
    }

    private Class<T> getRequestType() {
        return (Class<T>) requestType.getRawType();
    }

    private Class<U> getResponseType() {
        return (Class<U>) responseType.getRawType();
    }

    private V getSerializerInstance() {
        V instance = null;
        try {
            instance = ((Class<V>) serializerType.getRawType()).newInstance();
        } catch (Exception exception) {
            throw new ServiceConfigurationError("Unable to instantiate a serializer");
        }
        return instance;
    }

    public abstract Response<U> handleImpl(Request<T> requestT);
}

