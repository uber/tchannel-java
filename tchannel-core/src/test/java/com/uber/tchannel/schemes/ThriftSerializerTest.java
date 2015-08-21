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
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class ThriftSerializerTest {

    private Serializer.SerializerInterface serializer;

    @org.junit.Before
    public void setUp() throws Exception {
        this.serializer = new ThriftSerializer();
    }

    @Test
    public void testNewInstance() throws Exception {
        Class<?> instanceType = Ex.class;
        Ex obj = (Ex) instanceType.newInstance();
        assertNotNull(obj);
    }

    @Test
    public void testEncodeDecodeEndpoint() throws Exception {
        String endpoint = "SomeService::someMethod";
        ByteBuf endpointBuf = this.serializer.encodeEndpoint(endpoint);
        String decodedEndpoint = this.serializer.decodeEndpoint(endpointBuf);
        assertEquals(endpoint, decodedEndpoint);
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
    public void testEncodeDecodeBody() throws Exception {

        // Given
        Class<?> instanceType = Ex.class;
        Ex obj = (Ex) instanceType.newInstance();
        obj.setAnInteger(42);
        obj.setAString("Hello, World!");

        // Then
        ByteBuf bodyBuf = this.serializer.encodeBody(obj);
        Ex decodedObj = (Ex) this.serializer.decodeBody(bodyBuf, instanceType);

        assertEquals(obj.getAnInteger(), decodedObj.getAnInteger());
        assertEquals(obj.getAString(), decodedObj.getAString());
        assertEquals(obj, decodedObj);
    }

    @org.junit.After
    public void tearDown() throws Exception {
        this.serializer = null;
    }

}
