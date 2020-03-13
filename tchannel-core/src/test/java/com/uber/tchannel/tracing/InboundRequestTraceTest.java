package com.uber.tchannel.tracing;

import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.Request;
import io.jaegertracing.internal.JaegerSpanContext;
import io.jaegertracing.internal.JaegerTracer;
import io.jaegertracing.internal.reporters.InMemoryReporter;
import io.jaegertracing.internal.samplers.ConstSampler;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class InboundRequestTraceTest {
    private Tracer tracer = null;
    private Request request = null;

    @Before
    public void setUp() {
        tracer = new JaegerTracer.Builder("tchannel-name")
            .withReporter(new InMemoryReporter())
            .withSampler(new ConstSampler(true))
            .build();

        request = new JsonRequest
            .Builder<String>("tchannel-name", "endpoint")
            .setTimeout(1000, TimeUnit.MILLISECONDS)
            .setRetryLimit(0)
            .setBody("foo")
            .build();
    }

    @Test
    public void testInboundRequestWithNullTrace() {
        assertNull(request.getTrace());
        try {
            Span span =Tracing.startInboundSpan(request,tracer, new TracingContext.Default());
            assertNotNull(span);
        } catch (Exception e) {
            fail("Should not have thrown exception");
        }
    }

    @Test
    public void testInboundRequestWithNonNullTrace() {
        Trace trace = new Trace(42, 0, 42, (byte) 1);
        request.setTrace(trace);

        Span span = Tracing.startInboundSpan(request,tracer, new TracingContext.Default());
        JaegerSpanContext spanContext = (JaegerSpanContext) span.context();
        assertEquals(trace.traceId, spanContext.getTraceIdLow());
        assertEquals(trace.spanId, spanContext.getParentId());
        assertEquals(trace.traceFlags, spanContext.getFlags());
        assertNotEquals(trace.spanId, spanContext.getSpanId());
    }
}
