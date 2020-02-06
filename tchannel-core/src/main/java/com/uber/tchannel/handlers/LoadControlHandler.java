package com.uber.tchannel.handlers;

import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
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

    private final int low;
    private final int high;

    private int outstanding = 0;

    private LoadControlHandler(int low, int high) {
        this.low = low;
        this.high = high;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Request) {
            ChannelConfig config = ctx.channel().config();
            if (++outstanding >= high && config.isAutoRead()) {
                config.setAutoRead(false);
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof Response) {
            ChannelConfig config = ctx.channel().config();
            if (--outstanding <= low && !config.isAutoRead()) {
                config.setAutoRead(true);
            }
        }
        ctx.write(msg, promise);
    }

    public static final class Factory {

        /**
         * The high water mark should not be big.
         *
         * Queuing up outstanding requests means that the server cannot keep up with the incoming RPS.
         * Sooner or later, the high water mark will be reached. Prefer to do this sooner and avoid OOMs.
         *
         * A small high water mark is more sensitive to server 'hiccups'. These are resolved quickly (thus 'hiccups'),
         * so there is little harm.
         * Still, there can be some unnecessary CPU churn from triggering the high water mark on and off.
         */
        public static final int MAX_HIGH = 100;

        private final int low;
        private final int high;

        public Factory(int low, int high) {
            if (low < 0) {
                throw new IllegalArgumentException("invariant violation: low < 0");
            }
            if (high <= low) {
                throw new IllegalArgumentException("invariant violation: high <= low");
            }
            if (high > MAX_HIGH) {
                throw new IllegalArgumentException("invariant violation: high > " + MAX_HIGH);
            }
            this.low = low;
            this.high = high;
        }

        public LoadControlHandler create() {
            return new LoadControlHandler(low, high);
        }
    }
}
