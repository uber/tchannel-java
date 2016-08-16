package com.uber.tchannel.tracing;

import com.uber.jaeger.Span;
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
import io.opentracing.tag.Tags;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

/**
 * This test validates tracing context propagation through multiple network hops.
 */
@RunWith(Parameterized.class)
public class TracingPropagationTest {

    private static final String BAGGAGE_KEY = "baggage-key";

    private Tracer tracer;
    private InMemoryReporter reporter;
    private TracingContext tracingContext;
    private TChannel tchannel;
    private SubChannel subChannel;

    private final String forwardEncodings;
    private final boolean sampled;

    public TracingPropagationTest(String forwardEncodings, boolean sampled) {
        this.forwardEncodings = forwardEncodings;
        this.sampled = sampled;
    }

    @Parameters(name = "{index}: encodings({0}), sampled({1})")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"json,json", true},
                {"json,json", false},
                // { "json,thrift" },
        });
    }

    @Before
    public void setUp() throws Exception {
        reporter = new InMemoryReporter();
        Sampler sampler = new ConstSampler(true);
        tracer = new Tracer.Builder("tchannel-name", reporter, sampler).build();

        tracingContext = new TracingContext.ThreadLocal();

        tchannel = new TChannel.Builder("tchannel-name")
                .setServerHost(InetAddress.getByName("127.0.0.1"))
                .setTracer(tracer)
                .setTracingContext(tracingContext)
                .build();

        subChannel = tchannel.makeSubChannel("tchannel-name")
                .register("endpoint", new JSONHandler());

        tchannel.listen();
    }

    @After
    public void tearDown() {
        reporter.close();
        tchannel.shutdown();
    }

    class TraceResponse {
        String traceId;
        boolean sampled;
        String baggage;
        TraceResponse downstream;

        @Override
        public String toString() {
            return "TraceResponse{" +
                    "traceId='" + traceId + '\'' +
                    ", sampled=" + sampled +
                    ", baggage='" + baggage + '\'' +
                    ", downstream=" + downstream +
                    '}';
        }
    }

    private class JSONHandler extends JSONRequestHandler<String, TraceResponse> {

        @Override
        public JsonResponse<TraceResponse> handleImpl(JsonRequest<String> request) {
            String encodings = request.getBody(String.class);
            System.out.println("Received headers  : " + request.getHeaders());
            System.out.println("Received encodings: " + encodings);
            TraceResponse response = observeSpanAndDownstream(encodings);
            return new JsonResponse.Builder<TraceResponse>(request)
                    .setTransportHeaders(request.getTransportHeaders())
                    .setBody(response)
                    .build();
        }
    }

    private TraceResponse observeSpanAndDownstream(String encodings) {
        Span span = (Span) tracingContext.currentSpan();
        TraceResponse response = new TraceResponse();
        response.traceId = String.format("%x", span.getContext().getTraceID());
        response.sampled = span.getContext().isSampled();
        response.baggage = span.getBaggageItem(BAGGAGE_KEY);
        try {
            response.downstream = callDownstream(encodings);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    private TraceResponse callDownstream(String encodings) throws Exception {
        if (encodings.length() == 0) {
            return null;
        }
        int comma = encodings.indexOf(',');
        String encoding, remainingEncodings;
        if (comma == -1) {
            encoding = encodings;
            remainingEncodings = "";
        } else {
            encoding = encodings.substring(0, comma);
            remainingEncodings = encodings.substring(comma + 1);
        }
        if (encoding.equals("json")) {
            return callDownstreamJSON(remainingEncodings);
        } else {
            throw new IllegalArgumentException(encodings);
        }
    }

    private TraceResponse callDownstreamJSON(String remainingEncodings) throws Exception {
        JsonRequest<String> request = new JsonRequest
                .Builder<String>("tchannel-name", "endpoint")
                .setTimeout(2000000)
                .setBody(remainingEncodings)
                .build();

        TFuture<JsonResponse<TraceResponse>> responsePromise = subChannel.send(
                request,
                tchannel.getHost(),
                tchannel.getListeningPort()
        );

        JsonResponse<TraceResponse> response = responsePromise.get();
        assertNull(response.getError());
        TraceResponse resp = response.getBody(TraceResponse.class);
        response.release();
        return resp;
    }

    @Test
    public void testPropagation() throws Exception {
        System.out.println("forwardEncodings: " + forwardEncodings);

        Span span = (Span) tracer.buildSpan("root").start();
        String traceId = String.format("%x", span.getContext().getTraceID());
        String baggage = "Baggage-" + System.currentTimeMillis();
        span.setBaggageItem(BAGGAGE_KEY, baggage);
        if (!sampled) {
            Tags.SAMPLING_PRIORITY.set(span, (short) 0);
        }
        tracingContext.pushSpan(span);
        TraceResponse response = callDownstream(forwardEncodings);

        System.out.println("Final response: " + response);
        List<String> encodings = new ArrayList<>(Arrays.asList(forwardEncodings.split(",")));
        validate(encodings, traceId, baggage, response);
    }

    private void validate(List<String> encodings, String traceId, String baggage, TraceResponse response) {
        assertEquals(traceId, response.traceId);
        assertEquals(sampled, response.sampled);
        assertEquals(baggage, response.baggage);
        encodings.remove(0);
        if (encodings.isEmpty()) return;
        assertNotNull("Expecting downstream response", response.downstream);
        validate(encodings, traceId, baggage, response.downstream);
    }
}
