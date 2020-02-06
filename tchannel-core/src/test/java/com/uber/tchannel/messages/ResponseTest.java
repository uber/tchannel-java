package com.uber.tchannel.messages;

import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.messages.generated.Example;
import io.netty.util.IllegalReferenceCountException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResponseTest {

    @Test
    public void testCantSerializeBodyHardError() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();
        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(
            request)
            .setBody(new NonSerializable());
        //fail once
        try {
            builder.build();
            fail();
        } catch (Exception e) {
            //expected
        }
        assertNull(builder.arg2);
        assertNull(builder.arg3);

        //fail twice
        try {
            builder.build();
            fail();
        } catch (Exception e) {
            //expected
        }
        assertNull(builder.arg2);
        assertNull(builder.arg3);

        builder.setBody(new Example());

        ThriftResponse<Example> response = builder.build();
        assertNotNull(response.getArg1());
        assertNotNull(builder.arg2);
        assertNotNull(builder.arg3);

        assertTrue(builder.arg2 == response.arg2);
        assertTrue(builder.arg3 == response.arg3);
    }

    @Test
    public void testMissingResponseCode() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();
        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(
            request)
            .setBody(new Example()).setResponseCode(null);

        try {
            builder.build();
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("`responseCode` cannot be null."));
        }
        assertNull(builder.arg2);
        assertNull(builder.arg3);

        builder.setResponseCode(ResponseCode.OK);

        ThriftResponse<Example> response = builder.build();
        assertNotNull(response.getArg1());
        assertNotNull(builder.arg2);
        assertNotNull(builder.arg3);

        assertTrue(builder.arg2 == response.arg2);
        assertTrue(builder.arg3 == response.arg3);
    }

    @Test
    public void testReuseBuilder() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();

        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(request)
            .setBody(new Example());
        ThriftResponse<Example> resp1 = builder.build();
        ThriftResponse<Example> resp2 = builder.build();

        assertTrue(resp1 != resp2);
        assertEquals(resp1.getArg1(), resp2.getArg1());
        assertEquals(resp1.getArg2(), resp2.getArg2());
        assertEquals(resp1.getArg3(), resp2.getArg3());
    }

    @Test
    public void testReleaseSuccess() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();

        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(request)
            .setBody(new Example());
        ThriftResponse<Example> response = builder.build();
        assertNotNull(response.getArg1());
        assertNotNull(response.getArg2());
        assertNotNull(response.getArg3());


        response.release();

        //arg1 is static empty and has no effect
        assertNotNull(response.getArg1());
        assertNull(response.getArg2());
        assertNull(response.getArg3());
    }

    @Test
    public void testReleaseArg1Fail() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();

        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(request)
            .setBody(new Example());
        ThriftResponse<Example> response = builder.build();
        assertNotNull(response.getArg1());
        assertNotNull(response.getArg2());
        assertNotNull(response.getArg3());


        //arg1 is static empty and has no effect
        response.getArg1().release();
            response.release();

        assertNotNull(response.getArg1());
        assertNull(response.getArg2());
        assertNull(response.getArg3());
    }

    @Test
    public void testReleaseArg2Fail() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();

        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(request)
            .setBody(new Example());
        ThriftResponse<Example> response = builder.build();
        assertNotNull(response.getArg1());
        assertNotNull(response.getArg2());
        assertNotNull(response.getArg3());


        response.getArg2().release();
        try {
            response.release();
            fail();
        } catch (IllegalReferenceCountException ex) {
            //expected
        }

        assertNotNull(response.getArg1());
        assertNotNull(response.getArg2());
        assertNull(response.getArg3());
    }

    @Test
    public void testReleaseArg2Arg3Fail() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();

        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(request)
            .setBody(new Example());
        ThriftResponse<Example> response = builder.build();
        assertNotNull(response.getArg1());
        assertNotNull(response.getArg2());
        assertNotNull(response.getArg3());


        response.getArg2().release();
        response.getArg3().release();
        try {
            response.release();
            fail();
        } catch (IllegalReferenceCountException ex) {
            assertEquals(1, ex.getSuppressed().length);
            //expected
        }

        assertNotNull(response.getArg1());
        assertNotNull(response.getArg2());
        assertNotNull(response.getArg3());
    }

    @Test
    public void testReleaseArg3Fail() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();

        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(request)
            .setBody(new Example());
        ThriftResponse<Example> response = builder.build();
        assertNotNull(response.getArg1());
        assertNotNull(response.getArg2());
        assertNotNull(response.getArg3());


        response.getArg3().release();
        try {
            response.release();
            fail();
        } catch (IllegalReferenceCountException ex) {
            //expected
        }

        assertNotNull(response.getArg1());
        assertNull(response.getArg2());
        assertNotNull(response.getArg3());
    }

    public static class NonSerializable extends Example {

        @Override
        public void write(TProtocol oprot) throws TException {
            throw new RuntimeException("Can't write");
        }
    }
}
