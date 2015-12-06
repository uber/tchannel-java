package com.uber.tchannel.api;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.uber.tchannel.api.handlers.TFutureCallback;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.messages.Response;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class TFuture<V extends Response> extends AbstractFuture<V> {
    @SuppressWarnings({"rawtypes"})
    public static TFuture create(ArgScheme argScheme) {
        return new TFuture(argScheme);
    }

    private AtomicInteger listenerCount = new AtomicInteger(0);
    private V response = null;
    private ArgScheme argScheme = null;

    private TFuture(ArgScheme argScheme) {
        this.argScheme = argScheme;
    }

    @SuppressWarnings({"unchecked"})
    public void addCallback(final TFutureCallback<V> callback) {
        Futures.addCallback(this, new FutureCallback<V>() {
            @Override
            public void onSuccess(V response) {
                callback.onResponse(response);
            }

            @Override
            public void onFailure(Throwable throwable) {
                callback.onResponse(
                    (V) Response.build(
                        argScheme, 0, ErrorType.UnexpectedError, throwable.getMessage()
                    )
                );
            }
        });
    }

    @Override
    public boolean set(V response) {
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
        super.addListener(new Runnable() {
            @Override
            public void run() {
                listener.run();
                if (listenerCount.decrementAndGet() == 0) {
                    response.release();
                }
            }
        }, exec);
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