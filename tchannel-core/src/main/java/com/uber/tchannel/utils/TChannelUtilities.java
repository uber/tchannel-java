package com.uber.tchannel.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.io.PrintWriter;
import java.io.StringWriter;

public class TChannelUtilities {
    public static final ByteBuf emptyByteBuf = Unpooled.EMPTY_BUFFER;

    public static final void PrintException(Throwable throwable) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter( writer );
        throwable.printStackTrace( printWriter );
        printWriter.flush();
        System.out.println(throwable.getMessage());
        System.out.println(writer.toString());
    }
}
