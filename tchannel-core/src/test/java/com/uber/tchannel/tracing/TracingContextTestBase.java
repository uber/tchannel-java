package com.uber.tchannel.tracing;

import com.uber.jaeger.Tracer;
import com.uber.jaeger.reporters.InMemoryReporter;
import com.uber.jaeger.samplers.ConstSampler;
import io.opentracing.Span;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Test;

import java.util.EmptyStackException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Base for various {@link TracingContext} implementations tests.
 *
 * @author yegor 2018-02-13.
 */
public abstract class TracingContextTestBase {

    private final @NotNull InMemoryReporter reporter = new InMemoryReporter();

    @SuppressWarnings("resource") // closed in tearDown()
    private final @NotNull Tracer tracer = new Tracer.Builder(
        "tchannel-name", reporter, new ConstSampler(true)
    ).build();

    private final @NotNull TracingContext tracingContext;

    @SuppressWarnings("JUnitTestCaseWithNonTrivialConstructors")
    protected TracingContextTestBase() {
        tracingContext = tracingContext(tracer);
    }

    protected abstract @NotNull TracingContext tracingContext(@NotNull Tracer tracer);

    @After
    public void tearDown() {
        tracer.close();
        reporter.close();
    }

    @Test
    public void testTracingContextNoCurrent() {
        try {
            tracingContext.currentSpan();
            fail("Expected an EmptyStackException");
        } catch (EmptyStackException ignored) {}
    }

    @Test
    public void testTracingContextCannotPop() {
        try {
            tracingContext.popSpan();
            fail("Expected an EmptyStackException");
        } catch (EmptyStackException ignored) {}
    }

    @Test
    public void testTracingContext() {
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
    public void testTracingContextThreadLocal() throws InterruptedException {
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
