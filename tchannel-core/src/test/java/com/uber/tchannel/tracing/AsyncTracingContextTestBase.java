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
package com.uber.tchannel.tracing;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.uber.jaeger.reporters.NoopReporter;
import com.uber.jaeger.samplers.ConstSampler;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.api.handlers.HealthCheckRequestHandler;
import com.uber.tchannel.api.handlers.ThriftAsyncRequestHandler;
import com.uber.tchannel.messages.ErrorResponse;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import com.uber.tchannel.messages.generated.Meta;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.hamcrest.CoreMatchers;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;

/**
 * Base for tests of asynchronous request callback tracing context propagation for various implementations of
 * {@link TracingContext} interface.
 *
 * @author yegor 2017-11-10.
 */
public abstract class AsyncTracingContextTestBase {

    private static final String SERVICE = "service";

    private static final String META_HEALTH = "Meta::health";

    private static final String META_TEST = "Meta::test";

    protected abstract @NotNull TracingContext tracingContext(@NotNull Tracer tracer);

    @Test
    public void test() throws InterruptedException, TChannelError, ExecutionException, TimeoutException {
        Tracer tracer = new com.uber.jaeger.Tracer
            .Builder(SERVICE, new NoopReporter(), new ConstSampler(false))
            .build();
        final TracingContext tracingContext = tracingContext(tracer);
        try (
            final TestChannel service = new TestChannel(SERVICE, tracer, tracingContext);
        ) {
            final SubChannel clientChannel = service.clientChannel(SERVICE, service.addresses());

            service.register(META_HEALTH, new HealthCheckRequestHandler());

            service.register(META_TEST, new ThriftAsyncRequestHandler<Meta.health_args, Meta.health_result>() {
                @Override
                public ListenableFuture<ThriftResponse<Meta.health_result>> handleImpl(
                    ThriftRequest<Meta.health_args> request
                ) {
                    final SettableFuture<ThriftResponse<Meta.health_result>> resultFuture = SettableFuture.create();
                    try {
                        final Span span = tracingContext.currentSpan();
                        ListenableFuture<ThriftResponse<Meta.health_result>> responseFuture = clientChannel.send(
                            new ThriftRequest
                                .Builder<Meta.health_args>(SERVICE, META_HEALTH)
                                .setBody(new Meta.health_args())
                                .setTimeout(1000)
                                .setRetryLimit(0)
                                .build()
                        );
                        Futures.addCallback(
                            responseFuture,
                            new FutureCallback<ThriftResponse<Meta.health_result>>() {

                                @Override
                                public void onSuccess(
                                    @Nullable ThriftResponse<Meta.health_result> result
                                ) {
                                    try {
                                        Assert.assertThat(
                                            "Response callback context must have a current span",
                                            tracingContext.hasSpan(),
                                            is(true)
                                        );
                                        Assert.assertThat(
                                            "Response callback current span must be the same as the callback creator's",
                                            tracingContext.currentSpan(),
                                            sameInstance(span)
                                        );
                                        resultFuture.set(result);
                                    } catch (AssertionError testFailure) {
                                        // assertion error (if any) will re-surface when the future result is accessed
                                        resultFuture.setException(testFailure);
                                    }
                                }

                                @Override
                                public void onFailure(@NotNull Throwable t) {
                                    resultFuture.setException(t);
                                }

                            },
                            service.executor()
                        );
                    } catch (TChannelError tChannelError) {
                        resultFuture.setException(tChannelError);
                    }
                    return resultFuture;
                }
            });

            TFuture<ThriftResponse<Meta.health_result>> testResponseFuture = clientChannel.send(
                new ThriftRequest
                    .Builder<Meta.health_args>(SERVICE, META_TEST)
                    .setBody(new Meta.health_args())
                    .setTimeout(1000)
                    .setRetryLimit(0)
                    .build()
            );
            try (ThriftResponse<Meta.health_result> testResponse = testResponseFuture.get()) {
                Assert.assertThat(
                    "Response must have no errors",
                    testResponse.getError(),
                    CoreMatchers.nullValue(ErrorResponse.class)
                );
            }
        }
    }

}
