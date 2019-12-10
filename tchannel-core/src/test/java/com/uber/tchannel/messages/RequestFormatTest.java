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
import com.uber.tchannel.messages.generated.ExampleWithRequiredField;
import com.uber.tchannel.utils.TChannelUtilities;
import io.netty.buffer.ByteBuf;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RequestFormatTest {

    @Test
    public void testThriftHeader() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();
        ByteBuf arg1 = request.getArg1();
        ByteBuf arg2 = request.getArg2();
        ByteBuf arg3 = request.getArg3();
        assertEquals(2, arg2.readableBytes());
        assertEquals(0, arg2.getByte(0));
        assertEquals(0, arg2.getByte(1));
        assertNotNull(arg3);
        assertTrue(TChannelUtilities.emptyByteBuf == arg3);
        request.release();
        assertEquals(0, arg1.refCnt());
        assertEquals(0, arg2.refCnt());

        //still points to TChannelUtilities.emptyByteBuf
        assertEquals(1, arg3.refCnt());

        assertNull(request.getArg1());
        assertNull(request.getArg2());
        assertNull(request.getArg3());
    }

    @Test
    public void testJsonHeader() throws Exception {
        JsonRequest<Example> request = new JsonRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();
        ByteBuf arg2 = request.getArg2();
        assertEquals(2, arg2.readableBytes());

        // "{}"
        assertEquals(0x7b, arg2.getByte(0));
        assertEquals(0x7d, arg2.getByte(1));
        request.release();
    }

    @Test
    public void testRawHeader() throws Exception {
        RawRequest request = new RawRequest.Builder("keyvalue-service", "setValue")
            .build();
        ByteBuf arg2 = request.getArg2();
        assertEquals(0, arg2.readableBytes());
        request.release();
    }

    /**
     * Header serialization should not cause memory leaksl
     */
    @Test
    public void testCantSerializeHeaders() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put("key", null);
        ThriftRequest.Builder<ExampleWithRequiredField> requestBuilder =
            new ThriftRequest.Builder<ExampleWithRequiredField>(
                "keyvalue-service",
                "KeyValue::setValue"
            )
                .setBody(new ExampleWithRequiredField())
                .setHeaders(headers);

        try {
            requestBuilder.validate();
            fail("Expected exception");
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            assertTrue(exceptionAsString.contains("validateHeader"));
        }
        assertNotNull("Service endpoint initialized before validation stage", requestBuilder.getArg1());
        assertFalse("Arg1 should utilize Heap to avoid mem-leaks in validate", requestBuilder.getArg1().isDirect());
        assertFalse(
            "Arg1 should utilize Heap to avoid mem-leaks in validate",
            requestBuilder.getArg1().hasMemoryAddress()
        );
        assertTrue("Arg1 should utilize Heap to avoid mem-leaks in validate", requestBuilder.getArg1().hasArray());

        assertNull(requestBuilder.arg2);
        assertNull(requestBuilder.arg3);
    }

    /**
     * Body serialization should never fail
     */
    @Test
    public void testCantSerializeBody() throws Exception {
        ThriftRequest<ExampleWithRequiredField> request = new ThriftRequest.Builder<ExampleWithRequiredField>("keyvalue-service", "KeyValue::setValue")
            .setBody(new ExampleWithRequiredField())
            .build();
        ByteBuf arg1 = request.getArg1();
        ByteBuf arg2 = request.getArg2();
        ByteBuf arg3 = request.getArg3();
        assertEquals(2, arg2.readableBytes());
        assertEquals(0, arg2.getByte(0));
        assertEquals(0, arg2.getByte(1));
        assertNull(arg3);
        request.release();
        assertEquals(0, arg1.refCnt());
        assertEquals(0, arg2.refCnt());

        assertNull(request.getArg1());
        assertNull(request.getArg2());
        assertNull(request.getArg3());
    }
}
