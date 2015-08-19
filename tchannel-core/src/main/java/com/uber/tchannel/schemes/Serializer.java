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

package com.uber.tchannel.schemes;

import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.headers.TransportHeaders;
import io.netty.buffer.ByteBuf;

import java.util.Map;

public class Serializer {
    Map<ArgScheme, SerializerInterface> serializers;

    public Serializer(Map<ArgScheme, SerializerInterface> serializers) {
        this.serializers = serializers;
    }

    public String decodeEndpoint(RawMessage message) {
        return this.getSerializer(message).decodeEndpoint(message.getArg1());
    }

    public Map<String, String> decodeHeaders(RawMessage message) {
        return this.getSerializer(message).decodeHeaders(message.getArg2());
    }

    public <T> T decodeBody(RawMessage message, Class<T> bodyType) {
        return this.getSerializer(message).decodeBody(message.getArg3(), bodyType);
    }

    public ByteBuf encodeEndpoint(String method, ArgScheme argScheme) {
        return this.getSerializer(argScheme).encodeEndpoint(method);
    }

    public ByteBuf encodeHeaders(Map<String, String> applicationHeaders, ArgScheme argScheme) {
        return this.getSerializer(argScheme).encodeHeaders(applicationHeaders);
    }

    public <T> ByteBuf encodeBody(Object body, ArgScheme argScheme) {
        return this.getSerializer(argScheme).encodeBody(body);
    }

    private SerializerInterface getSerializer(RawMessage message) {
        Map<String, String> transportHeaders = message.getTransportHeaders();
        ArgScheme argScheme = ArgScheme.toScheme(transportHeaders.get(TransportHeaders.ARG_SCHEME_KEY));
        return this.serializers.get(argScheme);
    }

    private SerializerInterface getSerializer(ArgScheme argScheme) {
        return this.serializers.get(argScheme);
    }

    public interface SerializerInterface {

        String decodeEndpoint(ByteBuf arg1);

        Map<String, String> decodeHeaders(ByteBuf arg2);

        <T> T decodeBody(ByteBuf arg3, Class<T> bodyType);

        ByteBuf encodeEndpoint(String method);

        ByteBuf encodeHeaders(Map<String, String> applicationHeaders);

        ByteBuf encodeBody(Object body);
    }
}
