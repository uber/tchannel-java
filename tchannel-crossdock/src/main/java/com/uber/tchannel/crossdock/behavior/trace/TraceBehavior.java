package com.uber.tchannel.crossdock.behavior.trace;

import com.uber.jaeger.SpanContext;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.crossdock.api.Downstream;
import com.uber.tchannel.crossdock.api.ObservedSpan;
import com.uber.tchannel.crossdock.api.Request;
import com.uber.tchannel.crossdock.api.Response;
import com.uber.tchannel.headers.ArgScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.uber.tchannel.crossdock.Server.SERVER_NAME;

public class TraceBehavior {
    private static final Logger logger = LoggerFactory.getLogger(TraceBehavior.class);

    // Must match with corresponding constants in Go/Python/... projects
    public static final String BAGGAGE_KEY = "crossdock-baggage-key";

    final TChannel tchannel;
    private final ThriftHandler thriftHandler;
    private final JSONHandler jsonHandler;

    public TraceBehavior(TChannel tchannel) {
        this.tchannel = tchannel;
        this.thriftHandler = new ThriftHandler(this);
        this.jsonHandler = new JSONHandler(this);
        tchannel.makeSubChannel(SERVER_NAME)
                .register(ThriftHandler.ENDPOINT, thriftHandler)
                .register(JSONHandler.ENDPOINT, jsonHandler);
        logger.info("Registered TracingBehavior handlers");
    }

    public Response handleRequest(Request request) {
        ObservedSpan span = observeSpan();
        Response downstream = callDownstream(request.getDownstream());
        return new Response(span, downstream);
    }

    private ObservedSpan observeSpan() {
        if (!tchannel.getTracingContext().hasSpan()) {
            return new ObservedSpan("no span", false, "no span");
        }
        SpanContext spanContext = (SpanContext) tchannel.getTracingContext().currentSpan().context();

        return new ObservedSpan(
                String.format("%x", spanContext.getTraceID()),
                spanContext.isSampled(),
                spanContext.getBaggageItem(BAGGAGE_KEY));
    }

    private Response callDownstream(Downstream downstream) {
        if (downstream == null) {
            return null;
        }
        logger.info("Calling downstream {}", downstream);
        InetAddress host = host(downstream);
        int port = port(downstream);
        Response response;
        try {
            if (downstream.getEncoding().equals(ArgScheme.JSON.getScheme())) {
                response = jsonHandler.callDownstream(downstream, host, port);
            } else if (downstream.getEncoding().equals(ArgScheme.THRIFT.getScheme())) {
                response = thriftHandler.callDownstream(downstream, host, port);
            } else {
                throw new IllegalArgumentException("Unsupported encoding " + downstream.getEncoding());
            }
        } catch (Exception e) {
            logger.error("Failed to call downstream", e);
            return new Response(new ObservedSpan(e.toString(), false, e.toString()), null);
        }
        if (response == null) {
            response = new Response(
                    new ObservedSpan("Downstream call failed for " + downstream, false, ""),
                    null);
        }
        return response;
    }

    private InetAddress host(Downstream downstream) {
        String[] hostPort = downstream.getHostPort().split(":");
        try {
            return InetAddress.getByName(hostPort[0]);
        } catch (UnknownHostException e) {
            throw new RuntimeException(
                    "Cannot resolve host address for " + downstream.getHostPort(), e);
        }
    }

    private int port(Downstream downstream) {
        String[] hostPort = downstream.getHostPort().split(":");
        return Integer.parseInt(hostPort[1]);
    }
}
