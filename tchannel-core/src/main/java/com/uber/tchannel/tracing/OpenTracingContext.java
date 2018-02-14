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

import io.opentracing.Scope;
import io.opentracing.ScopeManager;
import io.opentracing.Span;
import org.jetbrains.annotations.NotNull;

import java.util.EmptyStackException;

/**
 * An implementation of {@link TracingContext} backed by OpenTracing {@link ScopeManager} for tracing context
 * propagation.
 *
 * @author yegor 2018-02-11.
 */
public class OpenTracingContext implements TracingContext {

    private final @NotNull ScopeManager scopeManager;

    /**
     * Constructs a new instance of {@link OpenTracingContext}.
     *
     * @param scopeManager
     *     a {@link ScopeManager} instance responsible for tracing context management; one can be obtained from
     *     {@link io.opentracing.Tracer#scopeManager()}
     */
    public OpenTracingContext(@NotNull ScopeManager scopeManager) {
        this.scopeManager = scopeManager;
    }

    @Override
    @SuppressWarnings("resource")
    public void pushSpan(@NotNull Span span) {
        scopeManager.activate(span, false);
    }

    @Override
    @SuppressWarnings("resource")
    public boolean hasSpan() {
        return scopeManager.active() != null;
    }

    @Override
    public @NotNull Span currentSpan() throws EmptyStackException {
        @SuppressWarnings("resource") Scope scope = scopeManager.active();
        if (scope == null) {
            throw new EmptyStackException();
        } else {
            return scope.span();
        }
    }

    @Override
    public @NotNull Span popSpan() throws EmptyStackException {
        try (Scope scope = scopeManager.active()) {
            if (scope == null) {
                throw new EmptyStackException();
            } else {
                return scope.span();
            }
        }
    }

    @Override
    @SuppressWarnings("resource")
    public void clear() {
        Scope scope;
        while ((scope = scopeManager.active()) != null) {
            scope.close();
        }
    }

}
