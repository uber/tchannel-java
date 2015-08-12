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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.uber.tchannel.messages.RawMessage;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.lang.reflect.Type;
import java.util.Map;

public final class JSONArgScheme {

    private static final Type HEADER_TYPE = (new TypeToken<Map<String, String>>() {

    }).getType();

    public static String decodeMethod(RawMessage request) {
        String endpoint = request.getArg1().toString(CharsetUtil.UTF_8);
        request.getArg1().release();
        return endpoint;
    }

    public static Map<String, String> decodeApplicationHeaders(RawMessage request) {
        String headerJSON = request.getArg2().toString(CharsetUtil.UTF_8);
        request.getArg2().release();
        return new Gson().fromJson(headerJSON, HEADER_TYPE);
    }

    public static Object decodeBody(RawMessage request, Type objectType) {
        String bodyJSON = request.getArg3().toString(CharsetUtil.UTF_8);
        request.getArg3().release();
        return new Gson().fromJson(bodyJSON, objectType);
    }

    public static RawResponse encodeResponse(JSONResponse jsonResponse, Type objectType) {

        String jsonMethod = jsonResponse.getMethod();
        String jsonHeaders = new Gson().toJson(jsonResponse.getApplicationHeaders());
        String jsonBody = new Gson().toJson(jsonResponse.getBody(), objectType);

        return new RawResponse(
                jsonResponse.getId(),
                jsonResponse.getTransportHeaders(),
                Unpooled.wrappedBuffer(jsonMethod.getBytes()),
                Unpooled.wrappedBuffer(jsonHeaders.getBytes()),
                Unpooled.wrappedBuffer(jsonBody.getBytes())
        );
    }

}
