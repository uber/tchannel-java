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

import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.reporters.InMemoryReporter;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopSpan;
import io.opentracing.noop.NoopTracerFactory;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import static org.junit.Assert.*;

/**
 * Tests {@link OpenTracingContext} implementation of {@link TracingContext} interface.
 *
 * @author yegor 2018-02-13.
 */
public class NoopTracingContextTest extends TracingContextTestBase {
    private TracingContext tracingContext;
    private Tracer tracer = new JaegerTracer.Builder("noop-tracing-context")
        .withReporter(new InMemoryReporter())
        .withSampler(new ConstSampler(true))
        .build();

    @Override
    protected @NotNull TracingContext tracingContext(@NotNull Tracer tracer) {
        tracingContext = new OpenTracingContext(NoopTracerFactory.create().scopeManager());
        return tracingContext;
    }

    @Override
    public void testTracingContextNoCurrent() {
        //NoopScopeManager always return default INSTANCE as active span
        assertTrue(tracingContext.hasSpan());
    }

    @Override
    public void testTracingContextCannotPop() {
        //NoopScopeManager always return default INSTANCE as active span which will be popped again and again
        assertNotNull(tracingContext.popSpan());
    }

    @Override
    public void testTracingContext() {
        tracingContext.clear();
        //make sure clear won't stuck in infinite loop
        Assert.assertTrue(true);
    }

    @Override
    public void testTracingContextThreadLocal() throws InterruptedException {
        final String[] spanId = new String[1];
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Span span = tracer.buildSpan("test").start();
                spanId[0] = span.context().toSpanId();
                tracingContext.pushSpan(span);
                assertEquals(NoopSpan.INSTANCE, tracingContext.currentSpan());
            }
        });
        thread.join();
        assertEquals(NoopSpan.INSTANCE, tracingContext.currentSpan());
    }
}
