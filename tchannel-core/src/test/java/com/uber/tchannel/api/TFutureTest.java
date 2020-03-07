package com.uber.tchannel.api;

import com.google.common.util.concurrent.MoreExecutors;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.messages.generated.Example;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TFutureTest {

    @Test
    public void testTfutureResponseReleasedAfterAllListeners() throws Exception {
        ThriftResponse<Example> response = prepareResponse();
        ByteBuf arg2 = response.getArg2();
        ByteBuf arg3 = response.getArg3();

        assertEquals(1, arg2.refCnt());
        assertEquals(1, arg3.refCnt());

        final TFuture<ThriftResponse<Example>> future = TFuture.create(ArgScheme.THRIFT, null);
        assertEquals(0, future.listenerCount.get());

        ExecutorService executor = Executors.newFixedThreadPool(1);

        future.addListener(new Runnable() {
            @Override
            public void run() {

            }
        }, executor);
        future.addListener(new Runnable() {
            @Override
            public void run() {
                throw new RuntimeException("error");
            }
        }, executor);
        future.addListener(new Runnable() {
            @Override
            public void run() {

            }
        }, executor);
        assertEquals(3, future.listenerCount.get());

        future.set(response);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        int retry = 0;
        while (future.listenerCount.intValue() > 0 && retry++ < 50) {
            Thread.sleep(100);
        }
        assertEquals(0, future.listenerCount.intValue());

        assertEquals(0, arg2.refCnt());
        assertEquals(0, arg3.refCnt());
        assertNull(response.getArg2());
        assertNull(response.getArg3());
    }

    @Test
    public void testTfutureGetInterrupted() throws Exception {
        ThriftResponse<Example> response = prepareResponse();
        ByteBuf arg2 = response.getArg2();
        ByteBuf arg3 = response.getArg3();

        assertEquals(1, arg2.refCnt());
        assertEquals(1, arg3.refCnt());

        final TFuture<ThriftResponse<Example>> future = TFuture.create(ArgScheme.THRIFT, null);
        assertEquals(0, future.listenerCount.get());

        future.set(response);

        Thread.currentThread().interrupt();
        try {
            future.get();
            fail();
        } catch (InterruptedException ex) {
            //expected
        }
        assertEquals(0, future.listenerCount.intValue());
        assertEquals(1, arg2.refCnt());
        assertEquals(1, arg3.refCnt());
        assertNotNull(response.getArg2());
        assertNotNull(response.getArg3());


        // add listener, will be executed right away and release() will be called
        future.addListener(new Runnable() {
            @Override
            public void run() {

            }
        }, MoreExecutors.directExecutor());

        assertEquals(0, arg2.refCnt());
        assertEquals(0, arg3.refCnt());
        assertNull(response.getArg2());
        assertNull(response.getArg3());
    }

    @NotNull
    private ThriftResponse<Example> prepareResponse() {
        ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("keyvalue-service", "KeyValue::setValue")
            .build();

        ThriftResponse.Builder<Example> builder = new ThriftResponse.Builder<Example>(request)
            .setBody(new Example());
        return builder.build();
    }
}
