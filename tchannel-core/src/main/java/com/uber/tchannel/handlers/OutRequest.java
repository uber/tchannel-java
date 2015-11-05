package com.uber.tchannel.handlers;

import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.messages.ErrorResponse;
import com.uber.tchannel.messages.Request;
import io.netty.util.Timeout;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The logic unit for managing out requests
 */
public final class OutRequest<V> {
    private final Request request;
    private final TFuture<V> future;

    private AtomicInteger retryCount = new AtomicInteger(0);
    private Timeout timeout = null;

    private ErrorResponse lastError = null;

    public OutRequest(Request request, TFuture<V> future) {
        this.request = request;
        this.future = future;
    }

    public Request getRequest() {
        return request;
    }

    public TFuture<V> getFuture() {
        return future;
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public boolean shouldRetry() {
        return this.retryCount.getAndIncrement() <= request.getRetryLimit();
    }

    public Timeout getTimeout() {
        return timeout;
    }

    public void setTimeout(Timeout timeout) {
        this.timeout = timeout;
    }

    public ErrorResponse getLastError() {
        return lastError;
    }

    public void setLastError(ErrorResponse lastError) {
        this.lastError = lastError;
    }
}
