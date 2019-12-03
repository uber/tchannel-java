package com.uber.tchannel.handlers;

import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.jetbrains.annotations.NotNull;

/**
 * Provides backpressure for a Netty pipeline.
 *
 * Keeps track of (#reads - #writes) as {@link #outstanding}.
 *
 * When the number of outstanding requests surpasses the {@link #high} water mark, reading is paused.
 *
 * When the number of outstanding requests goes below the {@link #low} water mark, reading is resumed.
 *
 * This signals the upstream producer (the client) to back off / slow down, because it will not be able to write any
 * more requests to the connection.
 *
 * Note: STATEFUL HANDLER (use new instance per pipeline)
 */
public final class LoadControlHandler extends ChannelDuplexHandler {

    private static final AtomicIntegerFieldUpdater<LoadControlHandler> OUTSTANDING_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(LoadControlHandler.class, "outstanding");

    private final ChannelConfig config;
    private final int low;
    private final int high;

    private volatile int outstanding = 0;

    private LoadControlHandler(@NotNull ChannelConfig config, int low, int high) {
        this.config = config;
        this.low = low;
        this.high = high;
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception {
        if (OUTSTANDING_UPDATER.incrementAndGet(this) >= high && config.isAutoRead()) {
            config.setAutoRead(false);
        }
        super.read(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (OUTSTANDING_UPDATER.decrementAndGet(this) <= low && !config.isAutoRead()) {
            config.setAutoRead(true);
        }
        super.write(ctx, msg, promise);
    }

    public static final class Factory {
        private final int low;
        private final int high;

        public Factory(int low, int high) {
            if (low < 0) {
                throw new IllegalArgumentException("invariant violation: low < 0");
            }
            if (high <= low) {
                throw new IllegalArgumentException("invariant violation: high <= low");
            }
            this.low = low;
            this.high = high;
        }

        public LoadControlHandler create(@NotNull ChannelConfig config) {
            return new LoadControlHandler(Objects.requireNonNull(config), low, high);
        }
    }
}
