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
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
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

    @Test
    public void testReleaseArg1Fail() throws Exception {
        RawRequest request = new RawRequest.Builder("keyvalue-service", "setValue").setBody("Body").setArg2(
            ByteBufAllocator.DEFAULT.buffer())
            .build();
        assertNotNull( request.arg1);
        assertNotNull( request.arg2);
        assertNotNull( request.arg3);

        //forcefully force release of arg1 to fail
        request.arg1.release();

        try {
            request.release();
            fail();
        } catch (IllegalReferenceCountException ex) {
            //expected
        }

        assertNotNull( request.arg1); // tried , but failed
        assertNull( request.arg2);
        assertNull( request.arg3);
    }

    @Test
    public void testReleaseArg2Fail() throws Exception {
        RawRequest request = new RawRequest.Builder("keyvalue-service", "setValue").setBody("Body").setArg2(
            ByteBufAllocator.DEFAULT.buffer())
            .build();
        assertNotNull( request.arg1);
        assertNotNull( request.arg2);
        assertNotNull( request.arg3);

        //forcefully force release of arg2 to fail
        request.arg2.release();

        try {
            request.release();
            fail();
        } catch (IllegalReferenceCountException ex) {
            //expected
        }

        assertNull( request.arg1);
        assertNotNull( request.arg2); // tried , but failed
        assertNull( request.arg3);
    }

    @Test
    public void testReleaseArg3Fail() throws Exception {
        RawRequest request = new RawRequest.Builder("keyvalue-service", "setValue").setBody("Body").setArg2(
            ByteBufAllocator.DEFAULT.buffer())
            .build();
        assertNotNull( request.arg1);
        assertNotNull( request.arg2);
        assertNotNull( request.arg3);

        //forcefully force release of arg3 to fail
        request.arg3.release();

        try {
            request.release();
            fail();
        } catch (IllegalReferenceCountException ex) {
            //expected
        }

        assertNull( request.arg1);
        assertNull( request.arg2);
        assertNotNull( request.arg3);// tried , but failed
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
     * Body serialization should never fail if serializable
     */
    @Test
    public void testCantSerializeBodySoftError() throws Exception {
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

    @Test
    public void testCantSerializeBodyHardError() throws Exception {
        ThriftRequest.Builder<Example> builder = new ThriftRequest.Builder<Example>(
            "keyvalue-service",
            "KeyValue::setValue"
        )
            .setBody(new NonSerializable());
        try {
            builder.build();
            fail();
        } catch (Exception e) {
            //expected
        }
        assertNotNull(builder.getArg1());
        assertNull(builder.arg2);
        assertNull(builder.arg3);
        assertNotNull(builder.headers);

        try {
            builder.build();
            fail();
        } catch (Exception e) {
            //expected
        }
        assertNotNull(builder.getArg1());
        assertNull(builder.arg2);
        assertNull(builder.arg3);
        assertNotNull(builder.headers);


        builder.setBody(new Example());
        ThriftRequest<Example> request = builder.build();
        assertNotNull(builder.getArg1());
        assertNotNull(builder.arg2);
        assertNotNull(builder.arg3);
        assertNotNull(builder.headers);

        assertTrue(builder.getArg1() == request.getArg1());
        assertTrue(builder.arg2 == request.arg2);
        assertTrue(builder.arg3 == request.arg3);
    }

    @Test
    public void testReuseBuilder() throws Exception {
        ThriftRequest.Builder<Example> builder = new ThriftRequest.Builder<Example>(
            "keyvalue-service",
            "KeyValue::setValue"
        )
            .setBody(new Example());
        ThriftRequest<Example> req1 = builder.build();
        ThriftRequest<Example> req2 = builder.build();

        assertTrue(req1 != req2);
        assertEquals(req1.getArg1(), req2.getArg1());
        assertEquals(req1.getArg2(), req2.getArg2());
        assertEquals(req1.getArg3(), req2.getArg3());
    }

    public static class NonSerializable extends Example {

        @Override
        public void write(TProtocol oprot) throws TException {
            throw new RuntimeException("Can't write");
        }
    }
}
