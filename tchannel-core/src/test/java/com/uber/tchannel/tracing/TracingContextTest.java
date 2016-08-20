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

import com.uber.jaeger.Tracer;
import com.uber.jaeger.reporters.InMemoryReporter;
import com.uber.jaeger.samplers.ConstSampler;
import com.uber.jaeger.samplers.Sampler;
import com.uber.tchannel.BaseTest;
import io.opentracing.Span;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.EmptyStackException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TracingContextTest extends BaseTest {
    private Tracer tracer;
    private InMemoryReporter reporter;
    private TracingContext tracingContext;

    @Before
    public void setUp() throws Exception {
        reporter = new InMemoryReporter();
        Sampler sampler = new ConstSampler(true);
        tracer = new Tracer.Builder("tchannel-name", reporter, sampler).build();

        tracingContext = new TracingContext.ThreadLocal();
    }

    @After
    public void tearDown() {
        reporter.close();
    }

    @Test(expected = EmptyStackException.class)
    public void testTracingContextNoCurrent() throws Exception {
        tracingContext.currentSpan();
    }

    @Test(expected = EmptyStackException.class)
    public void testTracingContextCannotPop() throws Exception {
        tracingContext.popSpan();
    }

    @Test
    public void testTracingContext() throws Exception {
        assertFalse(tracingContext.hasSpan());

        Span span = tracer.buildSpan("test").start();
        tracingContext.pushSpan(span);
        assertTrue(tracingContext.hasSpan());
        assertEquals(span, tracingContext.currentSpan());
        assertEquals(span, tracingContext.popSpan());
        assertFalse(tracingContext.hasSpan());

        Span span1 = tracer.buildSpan("test").start();
        Span span2 = tracer.buildSpan("test").start();
        tracingContext.pushSpan(span1);
        tracingContext.pushSpan(span2);
        assertEquals(span2, tracingContext.currentSpan());
        assertEquals(span2, tracingContext.popSpan());
        assertEquals(span1, tracingContext.popSpan());
        assertFalse(tracingContext.hasSpan());
    }

    @Test
    public void testTracingContextThreadLocal() throws Exception {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Span span = tracer.buildSpan("test").start();
                tracingContext.pushSpan(span);
                assertTrue("Have span in worker thread", tracingContext.hasSpan());
            }
        });
        thread.join();
        assertFalse("No span in main thread", tracingContext.hasSpan());
    }
}