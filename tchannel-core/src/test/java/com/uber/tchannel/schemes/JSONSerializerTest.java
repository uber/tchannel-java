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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JSONSerializerTest {

    private Serializer.SerializerInterface serializer;

    @Before
    public void setUp() throws Exception {
        this.serializer = new JSONSerializer();
    }

    @Test
    public void testEncodeDecodeEndpoint() throws Exception {

    }

    @Test
    public void testEncodeDecodeHeaders() throws Exception {

        Map<String, String> headers = new HashMap<String, String>() {{
            put("foo", "bar");
        }};

        ByteBuf binaryHeaders = serializer.encodeHeaders(headers);
        Map<String, String> decodedHeaders = serializer.decodeHeaders(binaryHeaders);

        assertEquals(headers, decodedHeaders);
    }

    @Test
    public void testDecodeEmptyHeaders() throws Exception {
        Map<String, String> emptyHeaders = new HashMap<>();
        Map<String, String> decodedHeaders = serializer.decodeHeaders(Unpooled.EMPTY_BUFFER);
        assertEquals(emptyHeaders, decodedHeaders);
    }

    @Test
    public void testEncodeEmptyHeaders() throws Exception {

        Map<String, String> emptyHeaders = new HashMap<>();
        ByteBuf encodedHeaders = serializer.encodeHeaders(emptyHeaders);
        assertEquals("{}", encodedHeaders.toString(CharsetUtil.UTF_8));

    }

    @Test
    public void testEncodeDecodeBody() throws Exception {

    }

    @After
    public void tearDown() throws Exception {
        this.serializer = null;
    }
}
