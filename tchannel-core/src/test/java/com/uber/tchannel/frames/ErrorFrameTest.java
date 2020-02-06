package com.uber.tchannel.frames;

import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.RawRequest;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertNull;

public class ErrorFrameTest {

    @Test
    public void sendErrorRequestReleased() {
        RawRequest req = new RawRequest.Builder("service", "endpoint").build();
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
        when(ctx.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));
        ErrorFrame.sendError(ErrorType.BadRequest, "bad request", req, ctx);
        assertNull(req.getArg1());
        assertNull(req.getArg2());
        assertNull(req.getArg3());
    }

    @Test
    public void sendErrorRequestAlreadyReleased() {
        RawRequest req = new RawRequest.Builder("service", "endpoint").build();
        req.release();
        assertNull(req.getArg1());
        assertNull(req.getArg2());
        assertNull(req.getArg3());

        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
        when(ctx.writeAndFlush(any())).thenReturn(mock(ChannelFuture.class));
        ErrorFrame.sendError(ErrorType.BadRequest, "bad request", req, ctx);
        assertNull(req.getArg1());
        assertNull(req.getArg2());
        assertNull(req.getArg3());
    }
}
