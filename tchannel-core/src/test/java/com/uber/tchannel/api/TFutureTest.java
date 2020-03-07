package com.uber.tchannel.api;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.messages.generated.Example;
import io.netty.buffer.ByteBuf;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TFutureTest {

    @Test
    public void testTfutureResponseReleasedAfterAllListeners() throws Exception {
        ThriftResponse<Example> response = prepareResponse();
        ByteBuf arg2 = response.getArg2();
        ByteBuf arg3 = response.getArg3();

        assertEquals(1, arg2.refCnt());
        assertEquals(1, arg3.refCnt());

        final AtomicReference<ThriftResponse<Example>> futureResponse1 = new AtomicReference<>();
        final AtomicReference<ThriftResponse<Example>> futureResponse2 = new AtomicReference<>();
        final TFuture<ThriftResponse<Example>> future = TFuture.create(ArgScheme.THRIFT, null);
        assertEquals(0, future.listenerCount.get());
        Futures.addCallback(future, new FutureCallback<ThriftResponse<Example>>() {
            @Override
            public void onSuccess(
                @Nullable ThriftResponse<Example> result
            ) {
                futureResponse1.set(result);
            }

            @Override
            public void onFailure(Throwable t) {

            }
        }, directExecutor());
        future.addCallback(new TFutureCallback<ThriftResponse<Example>>() {
            @Override
            public void onResponse(ThriftResponse<Example> response) {
                futureResponse2.set(response);
            }
        });

        future.addListener(new Runnable() {
            @Override
            public void run() {

            }
        }, directExecutor());
        assertEquals(3, future.listenerCount.get());

        future.set(response);

        ThriftResponse<Example> responseGet = future.get();
        assertEquals(1, future.listenerCount.get());

        assertEquals(response, responseGet);
        assertEquals(response, futureResponse1.get());
        assertEquals(response, futureResponse2.get());
        assertEquals(0, arg2.refCnt());
        assertEquals(0, arg3.refCnt());
        assertNull(response.getArg2());
        assertNull(response.getArg3());
    }


    @Test
    public void testTfutureResponseNotReleasedIfBlockingGetCalledFirst() throws Exception {
        ThriftResponse<Example> response = prepareResponse();
        ByteBuf arg2 = response.getArg2();
        ByteBuf arg3 = response.getArg3();

        assertEquals(1, arg2.refCnt());
        assertEquals(1, arg3.refCnt());

        final TFuture<ThriftResponse<Example>> future = TFuture.create(ArgScheme.THRIFT, null);

        future.set(response);

        ThriftResponse<Example> responseGet = future.get();
        assertEquals(1, future.listenerCount.get());
        assertEquals(response, responseGet);

        //listeners are executed right away if future is complete
        final AtomicReference<ThriftResponse<Example>> futureResponse = new AtomicReference<>();
        future.addCallback(new TFutureCallback<ThriftResponse<Example>>() {
            @Override
            public void onResponse(ThriftResponse<Example> response) {
                futureResponse.set(response);
                assertEquals(2, future.listenerCount.get());
            }
        });

        // listener executed right away, hence listener count is back to 1
        assertEquals(1, future.listenerCount.get());
        assertEquals(response, futureResponse.get());

        assertEquals(1, arg2.refCnt());
        assertEquals(1, arg3.refCnt());
        assertNotNull(response.getArg2());
        assertNotNull(response.getArg3());
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
