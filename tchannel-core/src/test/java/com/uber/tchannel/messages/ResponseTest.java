package com.uber.tchannel.messages;

import com.uber.tchannel.messages.generated.Example;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResponseTest {

    @Test
    public void testCantSerializeBodyHardError() throws Exception {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();
        ThriftResponse.Builder<NonSerializable> builder = new ThriftResponse.Builder<NonSerializable>(
            request)
            .setBody(new NonSerializable());
        try {
            builder.build();
            fail();
        } catch (Exception e) {
            //expected
        }
        assertNull(builder.arg2);
        assertNull(builder.arg3);

        try {
            builder.build();
            fail();
        } catch (Exception e) {
            //expected
        }
        assertNull(builder.arg2);
        assertNull(builder.arg3);
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

    public static class NonSerializable {

    }
}
