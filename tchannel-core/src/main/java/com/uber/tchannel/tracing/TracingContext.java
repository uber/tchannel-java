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

import io.opentracing.Span;

import java.util.EmptyStackException;
import java.util.Stack;

public interface TracingContext {
    /**
     * Push given span to the top of the stack.
     *
     * @param span new active span
     */
    void pushSpan(Span span);

    /**
     * @return whether there is a current/active span at the top of the stack
     */
    boolean hasSpan();

    /**
     * @return the current active span at the top of the stack, if any
     * @throws EmptyStackException when no active span has been pushed to the stack
     */
    Span currentSpan() throws EmptyStackException;

    /**
     * Remove the current active span from the top of the stack.
     * The next span in the stack, if any, becomes current.
     *
     * @return the current span from the top of the stack
     * @throws EmptyStackException when no active span has been pushed to the stack
     */
    Span popSpan() throws EmptyStackException;

    /**
     * Remove all spans from stack.
     */
    void clear();

    /**
     * Default implementation of TracingContext does not support in-process
     * propagation and therefore does not store any spans.
     */
    class Default implements TracingContext {

        @Override
        public void pushSpan(Span span) {
            // do nothing
        }

        @Override
        public boolean hasSpan() {
            return false;
        }

        @Override
        public Span currentSpan() {
            throw new EmptyStackException();
        }

        @Override
        public Span popSpan() throws EmptyStackException {
            throw new EmptyStackException();
        }

        @Override
        public void clear() {
            // do nothing
        }
    }

    class ThreadLocal implements TracingContext {

        private final java.lang.ThreadLocal<Stack<Span>> stack =
                new java.lang.ThreadLocal<Stack<Span>>() {
                    @Override
                    protected Stack<Span> initialValue() {
                        return new Stack<>();
                    }
                };

        private Stack<Span> stack() {
            return stack.get();
        }

        @Override
        public void pushSpan(Span span) {
            stack().push(span);
        }

        @Override
        public boolean hasSpan() {
            return !stack().isEmpty();
        }

        @Override
        public Span popSpan() {
            return stack().pop();
        }

        @Override
        public void clear() {
            stack().clear();
        }

        @Override
        public Span currentSpan() {
            return stack().peek();
        }
    }

}
