package com.uber.tchannel.api;

import com.google.common.util.concurrent.AbstractFuture;
import java.util.concurrent.TimeUnit;

public final class TFuture<V> extends AbstractFuture<V> {
    public static <V> TFuture<V> create() {
        return new TFuture();
    }

    private TFuture() {
    }

    public boolean set(V value) {
        return super.set(value);
    }

    public boolean setException(Throwable throwable) {
        return super.setException(throwable);
    }

    @Override
    public V get(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Get timeout is unsupported. Use request timeout instead.");
    }
}