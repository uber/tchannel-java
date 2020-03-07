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

package com.uber.tchannel.api;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.tracing.TracingContext;
import io.opentracing.Span;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EmptyStackException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class representing the future response of a TChannel call. It is generic
 * and wraps/contains a payload. You construct instances via the factory
 * {@link #create(ArgScheme, TracingContext)}.
 * @param <V> the type of the payload
 */

public final class TFuture<V extends Response> extends AbstractFuture<V> {

    private static final Logger logger = LoggerFactory.getLogger(TFuture.class);

    /**
     * Create future. Example usage: {@code TFuture<RawResponse> future = TFuture.create(...); }.
     */
    public static @NotNull <T extends Response> TFuture<T> create(
        ArgScheme argScheme,
        @Nullable TracingContext tracingContext
    ) {
        return new TFuture<>(argScheme, tracingContext);
    }

    /** @deprecated Use {@link #create(ArgScheme, TracingContext)}. */
    @Deprecated
    public static @NotNull <T extends Response> TFuture<T> create(ArgScheme argScheme) {
        return create(argScheme, null);
    }

    private final AtomicInteger listenerCount = new AtomicInteger(0);
    private final ArgScheme argScheme;
    private final @Nullable TracingContext tracingContext;
    private V response = null;

    private TFuture(ArgScheme argScheme, @Nullable TracingContext tracingContext) {
        this.argScheme = argScheme;
        this.tracingContext = tracingContext;
    }

    @SuppressWarnings("unchecked")
    public void addCallback(final TFutureCallback<V> callback) {
        Futures.addCallback(this, new FutureCallback<V>() {
            @Override
            public void onSuccess(V response) {
                callback.onResponse(response);
            }

            @Override
            public void onFailure(@NotNull Throwable throwable) {
                callback.onResponse(
                    (V) Response.build(
                        argScheme, 0, ErrorType.UnexpectedError, throwable.getMessage()
                    )
                );
            }
           // TODO: use proper executor, for now directExecutor provides
           // legacy behaviour and removes usage of deprecated method
        }, MoreExecutors.directExecutor());
    }

    @Override
    public boolean set(V response) {
        // Error doesn't need to be released
        if (listenerCount.get() == 0 && !response.isError()) {
            logger.warn(
                "No handler is set when response is set. Resource leak may occur.",
                new IllegalStateException() // log the stacktrace
            );
        }

        this.response = response;
        return super.set(response);
    }

    @Override
    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }

    @Override
    public void addListener(final Runnable listener, Executor exec) {
        listenerCount.incrementAndGet();
        // this is the current span of whomever is adding the listener - preserve it for the invocation of the latter
        final Span span = tracingContext != null && tracingContext.hasSpan() ? tracingContext.currentSpan() : null;
        super.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        pushSpan(span);
                        listener.run();
                    } finally {
                        popSpan(span, listener);
                    }
                } finally {
                    if (listenerCount.decrementAndGet() <= 0) {
                        response.release();
                    }
                }
            }
        }, exec);
    }

    private void popSpan(Span span, Runnable listener) {
        if (span != null) {
            try { // this _might_ fail in case the listener managed to corrupt the tracing context
                Span poppedSpan = tracingContext.popSpan();
                if (!span.equals(poppedSpan)) {
                    logger.error(
                        "Corrupted tracing context after running listener {}: expected span {} but got {}",
                        listener, span, poppedSpan
                    );
                }
            } catch (EmptyStackException e) {
                logger.error("Corrupted (empty) tracing context after running listener {}", listener, e);
            }
        }
    }

    private void pushSpan(Span span) {
        if (span != null) {
            tracingContext.pushSpan(span);
        }
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        listenerCount.incrementAndGet();
        return super.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Get timeout is unsupported. Use request timeout instead.");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Cancel is not supported.");
    }

}
