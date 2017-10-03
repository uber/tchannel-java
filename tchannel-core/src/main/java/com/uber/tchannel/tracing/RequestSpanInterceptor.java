package com.uber.tchannel.tracing;

import com.uber.tchannel.messages.Request;
import io.opentracing.Span;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link TracingContext} implementation decorated with this interface will get an option to intercept an inbound
 * and/or outgoing request right after the tracing {@link Span} has been created and populated.
 */
public interface RequestSpanInterceptor {

    /**
     * This method will be invoked right after the inbound request span has been created, allowing the implementation to
     * inspect the request and/or additional baggage from the span context.
     *
     * @throws RuntimeException
     *     if the inbound request should fail immediately
     */
    void interceptInbound(@NotNull Request request, @NotNull Span span) throws RuntimeException;

    /**
     * This method will be invoked right after the outgoing request span has been created, allowing the implementation
     * to inspect the request and/or attach additional baggage to the span context.
     *
     * @throws RuntimeException
     *     if the outbound request should fail immediately
     */
    void interceptOutbound(@NotNull Request request, @NotNull Span span) throws RuntimeException;

}
