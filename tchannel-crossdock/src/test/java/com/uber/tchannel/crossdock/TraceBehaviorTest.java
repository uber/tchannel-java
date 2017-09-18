
package com.uber.tchannel.crossdock;

import com.uber.jaeger.SpanContext;
import com.uber.jaeger.Tracer;
import com.uber.jaeger.reporters.CompositeReporter;
import com.uber.jaeger.reporters.InMemoryReporter;
import com.uber.jaeger.reporters.LoggingReporter;
import com.uber.jaeger.reporters.Reporter;
import com.uber.jaeger.samplers.ConstSampler;
import com.uber.jaeger.samplers.Sampler;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.crossdock.api.Downstream;
import com.uber.tchannel.crossdock.api.Request;
import com.uber.tchannel.crossdock.api.Response;
import com.uber.tchannel.crossdock.behavior.trace.TraceBehavior;
import io.opentracing.Span;
import io.opentracing.tag.Tags;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.uber.tchannel.crossdock.Server.SERVER_NAME;
import static com.uber.tchannel.crossdock.behavior.trace.TraceBehavior.BAGGAGE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * TODO
 * - install thrift via docker
 * - matrix for java 7, 8
 */
@RunWith(Parameterized.class)
public class TraceBehaviorTest {
    private static final Logger logger = LoggerFactory.getLogger(TraceBehaviorTest.class);

    private Server server;
    private TChannel tchannel;
    private TraceBehavior traceBehavior;

    private InMemoryReporter reporter;
    private Tracer tracer;

    private final String encoding1;
    private final String encoding2;
    private final boolean sampled;

    public TraceBehaviorTest(String encoding1, String encoding2, boolean sampled) {
        this.encoding1 = encoding1;
        this.encoding2 = encoding2;
        this.sampled = sampled;
    }

    @Parameterized.Parameters(name = "{index}: encodings({0}, {1}), sampled({2})")
    public static Collection<Object[]> data() {
        String[] encodings = new String[] { "json", "thrift" };
        boolean[] sampling = new boolean[] { true, false };
        List<Object[]> data = new ArrayList<>();
        for (String encoding1 : encodings) {
            for (String encoding2 : encodings) {
                for (boolean sampled: sampling) {
                    data.add(new Object[] { encoding1, encoding2, sampled });
                }
            }
        }
        return data;
    }

    @Before
    public void setUp() throws Exception {
        reporter = new InMemoryReporter();
        Sampler sampler = new ConstSampler(false);
        Reporter compositeReporter = new CompositeReporter(reporter, new LoggingReporter());
        tracer = new Tracer.Builder(SERVER_NAME, compositeReporter, sampler).build();

        server = new Server("127.0.0.1", tracer);
        server.start();
        tchannel = server.tchannel;
        traceBehavior = server.traceBehavior;
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
        reporter.close();
    }

    @Test
    public void TestTraceBehavior() throws Exception {
        logger.info("Starting test encoding1={}, encoding2={}, sampled={}", encoding1, encoding2, sampled);

        String host = tchannel.getListeningHost();
        Downstream request = new Downstream(
                SERVER_NAME,
                "s3",
                encoding1,
                host + ":" + tchannel.getPort(),
                new Downstream(
                        SERVER_NAME,
                        "s4",
                        encoding2,
                        host + ":" + tchannel.getPort(),
                        null
                )
        );

        String baggage = "luggage-" + System.currentTimeMillis();
        Span span = tracer.buildSpan("root").startManual();
        span.setBaggageItem(BAGGAGE_KEY, baggage);
        if (sampled) {
            Tags.SAMPLING_PRIORITY.set(span, 1);
        }
        tchannel.getTracingContext().pushSpan(span);

        Response response = traceBehavior.handleRequest(new Request("s2", request));
        logger.info("Response: {}", response);
        span.finish();

        SpanContext spanContext = (SpanContext) span.context();
        String traceId = String.format("%x", spanContext.getTraceId());

        validate(response, traceId, baggage, 2);
        if (sampled) {
            for (int i = 0; i < 100; i++) {
                if (reporter.getSpans().size() == 5) {
                    break;
                }
                Thread.sleep(10);
            }
            assertEquals(5, reporter.getSpans().size());
        }
    }

    private void validate(Response response, String traceId, String baggage, int downstreamCount) {
        assertNotNull(response);
        assertNotNull(response.getSpan());
        assertEquals(traceId, response.getSpan().getTraceId());
        assertEquals(sampled, response.getSpan().isSampled());
        assertEquals(baggage, response.getSpan().getBaggage());
        if (downstreamCount > 0) {
            assertNotNull(response.getDownstream());
            validate(response.getDownstream(), traceId, baggage, downstreamCount - 1);
        }
    }
}