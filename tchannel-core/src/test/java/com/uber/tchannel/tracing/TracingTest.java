package com.uber.tchannel.tracing;

import com.uber.jaeger.Tracer;
import com.uber.jaeger.reporters.InMemoryReporter;
import com.uber.jaeger.samplers.ConstSampler;
import com.uber.jaeger.samplers.Sampler;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.JSONRequestHandler;
import com.uber.tchannel.messages.JsonRequest;
import com.uber.tchannel.messages.JsonResponse;
import io.opentracing.Span;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TracingTest {

    private Tracer tracer;
    private InMemoryReporter reporter;

    @Before
    public void setUp() {
        reporter = new InMemoryReporter();
        Sampler sampler = new ConstSampler(true);
        tracer = new Tracer.Builder("tchannel-name", reporter, sampler).build();
    }

    @After
    public void tearDown() {
        reporter.close();
    }

    @Test
    public void testOutbound() throws Exception {
        final String requestBody = "Hello, World!";

        final TracingContext.ThreadLocal tracingContext = new TracingContext.ThreadLocal();

        TChannel tchannel = new TChannel.Builder("tchannel-name")
                .setServerHost(InetAddress.getByName("127.0.0.1"))
                .setTracer(tracer)
                .setTracingContext(tracingContext)
                .build();
        // attach JSON server handler
        SubChannel subChannel = tchannel.makeSubChannel("tchannel-name")
                .register("endpoint", new JSONRequestHandler<String, String>() {
                    public JsonResponse<String> handleImpl(JsonRequest<String> request) {
                        System.out.println(request.getHeaders());
                        assertEquals(requestBody, request.getBody(String.class));
                        Span span = tracingContext.currentSpan();
                        return new JsonResponse.Builder<String>(request)
                                .setTransportHeaders(request.getTransportHeaders())
                                .setBody(span.getBaggageItem("baggage-key"))
                                .build();
                    }
                });

        tchannel.listen();

        JsonRequest<String> request = new JsonRequest
                .Builder<String>("tchannel-name", "endpoint")
                .setTimeout(2000000)
                .setBody(requestBody)
                .setHeader("x", "y")
                .build();

        Span span = tracer.buildSpan("root").start();
        span.setBaggageItem("baggage-key", "Baggage Value");
        tracingContext.clear();
        tracingContext.pushSpan(span);

        TFuture<JsonResponse<String>> responsePromise = subChannel.send(
                request,
                tchannel.getHost(),
                tchannel.getListeningPort()
        );

        JsonResponse<String> response = responsePromise.get();
        assertNull(response.getError());
        assertEquals("Baggage Value", response.getBody(String.class));
        response.release();

        tchannel.shutdown();
    }
}
