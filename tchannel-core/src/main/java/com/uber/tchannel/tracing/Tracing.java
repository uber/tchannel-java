package com.uber.tchannel.tracing;

import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.handlers.OutRequest;
import com.uber.tchannel.messages.EncodedRequest;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static io.opentracing.References.CHILD_OF;

public class Tracing {

    private static final Logger logger = LoggerFactory.getLogger(Tracing.class);

    public static void startOutboundSpan(
            OutRequest outRequest,
            Tracer tracer,
            TracingContext tracingContext
    ) {
        if (tracer == null) {
            return;
        }
        Request request = outRequest.getRequest();
        Tracer.SpanBuilder builder = tracer.buildSpan(request.getEndpoint());
        if (tracingContext.hasSpan()) {
            Span parentSpan = tracingContext.currentSpan();
            builder.asChildOf(parentSpan.context());
        }
        // TODO add tags for peer host:port
        builder
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
                .withTag(Tags.PEER_SERVICE.getKey(), request.getService())
                .withTag("as", request.getArgScheme().name());

        final Span span = builder.start();

        // TODO if tracer is Zipkin compatible, inject Trace fields
        // if request has headers, inject tracing context
        if (request instanceof TraceableRequest) {
            TraceableRequest traceableRequest = (TraceableRequest) request;
            //Format.Builtin.TEXT_MAP
            Map<String, String> headers = traceableRequest.getHeaders();
            PrefixedHeadersCarrier carrier = new PrefixedHeadersCarrier(headers);
            try {
                tracer.inject(span.context(), Format.Builtin.TEXT_MAP, carrier);
                traceableRequest.updateHeaders(headers);
            } catch (Exception e) {
                logger.error("Failed to inject span context into headers", e);
            }
        }
        outRequest.getFuture().addCallback(new TFutureCallback() {
            @Override
            public void onResponse(Response response) {
                if (response.isError()) {
                    Tags.ERROR.set(span, true);
                    span.log(response.getError().getMessage(), null);
                }
                span.finish();
            }
        });
    }

    public static Span startInboundSpan(
            Request request,
            Tracer tracer,
            TracingContext tracingContext) {
        SpanContext parentContext = null;
        if (request instanceof TraceableRequest) {
            TraceableRequest traceableRequest = (TraceableRequest) request;
            Map<String, String> headers = traceableRequest.getHeaders();
            PrefixedHeadersCarrier carrier = new PrefixedHeadersCarrier(headers);
            try {
                parentContext = tracer.extract(Format.Builtin.TEXT_MAP, carrier);
                Map<String, String> nonTracingHeaders = carrier.getNonTracingHeaders();
                if (nonTracingHeaders.size() < headers.size()) {
                    traceableRequest.updateHeaders(nonTracingHeaders);
                }
            } catch (Exception e) {
                logger.error("Failed to extract span context from headers", e);
            }
        } else {
            // TODO if tracer is Zipkin compatible, extract parent from Trace fields
        }
        Tracer.SpanBuilder builder = tracer.buildSpan(request.getEndpoint());
        builder
                .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
                .withTag("as", request.getArgScheme().name());
        Map<String, String> transportHeaders = request.getTransportHeaders();
        if (transportHeaders != null && transportHeaders.containsKey("cn")) {
            builder.withTag(Tags.PEER_SERVICE.getKey(), transportHeaders.get("cn"));
        }
        // TODO add tags for peer host:port
        if (parentContext != null) {
            builder.asChildOf(parentContext);
        }
        Span span = builder.start();
        tracingContext.clear();
        tracingContext.pushSpan(span);
        return span;
    }
}
