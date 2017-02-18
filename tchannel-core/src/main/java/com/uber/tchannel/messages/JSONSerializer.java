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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public final class JSONSerializer implements Serializer.SerializerInterface {
    private static final Type HEADER_TYPE = (new TypeToken<Map<String, String>>() {

    }).getType();

    private static final Gson GSON = new Gson();

    @Override
    public String decodeEndpoint(ByteBuf arg1) {
        return arg1.toString(CharsetUtil.UTF_8);
    }

    @Override
    public Map<String, String> decodeHeaders(ByteBuf arg2) {
        String headerJSON = arg2.toString(CharsetUtil.UTF_8);
        Map<String, String> headers = null;
        if (headerJSON != null && !headerJSON.isEmpty() && !headerJSON.equals("\"\"")) {
            headers = new Gson().fromJson(headerJSON, HEADER_TYPE);
        }

        return (headers == null) ? new HashMap<String, String>() : headers;
    }

    @Override
    public <T> T decodeBody(ByteBuf arg3, Class<T> bodyType) {
        String bodyJSON = arg3.toString(CharsetUtil.UTF_8);
        return GSON.fromJson(bodyJSON, bodyType);
    }

    @Override
    public ByteBuf encodeEndpoint(String method) {

        return Unpooled.wrappedBuffer(method.getBytes());
    }

    @Override
    public ByteBuf encodeHeaders(Map<String, String> applicationHeaders) {
        return Unpooled.wrappedBuffer(GSON.toJson(applicationHeaders, HEADER_TYPE).getBytes());
    }

    @Override
    public ByteBuf encodeBody(Object body) {
        return Unpooled.wrappedBuffer(GSON.toJson(body).getBytes());
    }

}
