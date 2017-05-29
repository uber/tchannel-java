/*
 * Copyright (c) 2016 Uber Technologies, Inc.
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
package com.uber.tchannel.handlers;

import com.uber.jaeger.Tracer;
import com.uber.jaeger.reporters.InMemoryReporter;
import com.uber.jaeger.samplers.ConstSampler;
import com.uber.jaeger.samplers.Sampler;
import com.uber.tchannel.tracing.TracingContext;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.BaseTest;
import com.uber.tchannel.api.handlers.ThriftAsyncRequestHandler;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.messages.generated.Example;

import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestThriftAsyncRequestHandler extends BaseTest {

    @Test
    public void fullRequestResponse() throws Exception {
        final Example requestBody = new Example("Hello, World!", 10);
        final Example responseBody = new Example("Bonjour le monde!", 20);

        TChannel tchannel = new TChannel.Builder("tchannel-name")
            .setServerHost(InetAddress.getByName("127.0.0.1"))
            .build();
        SubChannel subChannel = tchannel.makeSubChannel("tchannel-name")
            .register("endpoint", new ThriftAsyncRequestHandler<Example, Example>() {
                @Override
                public ListenableFuture<ThriftResponse<Example>> handleImpl(ThriftRequest<Example> request) {
                    assertEquals(requestBody, request.getBody(Example.class));
                    return Futures.immediateFuture(new ThriftResponse.Builder<Example>(request)
                        .setTransportHeaders(request.getTransportHeaders())
                        .setBody(responseBody)
                        .build());
                }
        });

        tchannel.listen();

        ThriftResponse<Example> response = null;
        try {
            ThriftRequest<Example> request = new ThriftRequest.Builder<Example>("tchannel-name", "endpoint")
                .setTimeout(2000000)
                .setBody(requestBody)
                .build();

            TFuture<ThriftResponse<Example>> responsePromise = subChannel.send(
                request,
                tchannel.getHost(),
                tchannel.getListeningPort()
            );

            response = responsePromise.get();
            assertNull(response.getError());
            assertEquals(responseBody, (Example) response.getBody(Example.class));
        } finally {
            if (response != null) {
                response.release();
            }
            tchannel.shutdown();
        }
    }
}
