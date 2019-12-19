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

import com.uber.tchannel.messages.generated.Example;
import io.netty.buffer.ByteBuf;
import io.netty.util.internal.PlatformDependent;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestRule;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ThriftSerializerTest {

    @Rule
    public final TestRule restoreSystemProperties
        = new RestoreSystemProperties();

    private Serializer.SerializerInterface serializer;

    @org.junit.Before
    public void setUp() {
        this.serializer = new ThriftSerializer();
        ThriftSerializer.init();
    }

    @After
    public void after() {
        ThriftSerializer.init();
    }

    @Test
    public void testNewInstance() throws Exception {
        Class<Example> instanceType = Example.class;
        Example obj = instanceType.getConstructor().newInstance();
        assertNotNull(obj);
    }

    @Test
    public void testEncodeDecodeEndpoint() {
        String endpoint = "SomeService::someMethod";
        ByteBuf endpointBuf = this.serializer.encodeEndpoint(endpoint);
        String decodedEndpoint = this.serializer.decodeEndpoint(endpointBuf);
        assertEquals(endpoint, decodedEndpoint);
        endpointBuf.release();
    }

    @Test
    public void testEncodeDecodeHeaders() {
        Map<String, String> headers = new HashMap<String, String>() {{
            put("foo", "bar");
        }};

        ByteBuf binaryHeaders = serializer.encodeHeaders(headers);
        Map<String, String> decodedHeaders = serializer.decodeHeaders(binaryHeaders);
        assertEquals(headers, decodedHeaders);
        binaryHeaders.release();
    }

    @Test
    public void testEncodeDecodeEmptyHeaders() {
        Map<String, String> emptyHeaders = new HashMap<String, String>(){{
            put("key", "value");
        }};
        ByteBuf binaryHeaders = serializer.encodeHeaders(emptyHeaders);
        assertTrue(binaryHeaders.isDirect());
        assertFalse(binaryHeaders.hasArray());

        Map<String, String> decodedHeaders = serializer.decodeHeaders(binaryHeaders);
        assertEquals(emptyHeaders, decodedHeaders);
        binaryHeaders.release();
    }

    @Test
    public void testEncodeDecodeInvalidHeaders() {
        Map<String, String> emptyHeaders = new HashMap<>();
        emptyHeaders.put("key", null);
        try {
            ByteBuf binaryHeaders = serializer.encodeHeaders(emptyHeaders);
            fail();
        } catch (Exception e) {
            //expected
        }
    }

    @Test
    public void testEmptyHeadersBackedByHeapConstantByteBuf() {
        Map<String, String> emptyHeaders = new HashMap<>();
        ByteBuf binaryHeaders = serializer.encodeHeaders(emptyHeaders);
        assertTrue(binaryHeaders.hasArray());
        assertFalse(binaryHeaders.isDirect());
    }

    @Test
    public void testDirectBufferPreferred() {
        assertEquals(PlatformDependent.directBufferPreferred(), ThriftSerializer.directBufferPreferred());
        assertTrue(ThriftSerializer.directBufferPreferred());
    }

    @Test
    public void testOverrideDirectBufferPreferred() {
        System.setProperty("com.uber.tchannel.thrift_serializer.noPreferDirect", "true");
        ThriftSerializer.init();
        assertFalse(ThriftSerializer.directBufferPreferred());

        ByteBuf binaryHeaders = serializer.encodeHeaders(new HashMap<String, String>() {
            {
                put("key", "value");
            }
        });
        assertTrue(binaryHeaders.hasArray());
        assertFalse(binaryHeaders.isDirect());

        binaryHeaders.release();

        try {
            serializer.encodeHeaders(new HashMap<String, String>() {
                {
                    put("key", null);
                }
            });
            fail();
        } catch (Exception e) {
            //expected
        }
    }

    @Test
    public void testEncodeDecodeBody() throws Exception {

        // Given
        Class<Example> instanceType = Example.class;
        Example obj = instanceType.getConstructor().newInstance();
        obj.setAnInteger(42);
        obj.setAString("Hello, World!");

        // Then
        ByteBuf bodyBuf = this.serializer.encodeBody(obj);
        Example decodedObj = (Example) this.serializer.decodeBody(bodyBuf, instanceType);

        assertEquals(obj.getAnInteger(), decodedObj.getAnInteger());
        assertEquals(obj.getAString(), decodedObj.getAString());
        assertEquals(obj, decodedObj);
        bodyBuf.release();
    }

    @org.junit.After
    public void tearDown() {
        this.serializer = null;
    }
}
