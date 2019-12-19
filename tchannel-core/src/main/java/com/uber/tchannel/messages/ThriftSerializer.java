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

package com.uber.tchannel.messages;

import com.google.common.annotations.VisibleForTesting;
import com.uber.tchannel.codecs.CodecUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;

import io.netty.util.internal.PlatformDependent;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ThriftSerializer implements Serializer.SerializerInterface {

    private static final Logger logger = LoggerFactory.getLogger(ThriftSerializer.class);

    private static ByteBuf EMPTY_HEADER_BYTEBUF;

    private static boolean DIRECT_BUFFER_PREFERRED;

    static {
        init();
    }

    @VisibleForTesting
    static void init()
    {
        // We should always prefer direct buffers by default if Netty prefers it and not explicitly prohibited.
        DIRECT_BUFFER_PREFERRED = PlatformDependent.directBufferPreferred()
            && !SystemPropertyUtil.getBoolean("com.uber.tchannel.thrift_serializer.noPreferDirect", false);
        if (logger.isDebugEnabled()) {
            logger.debug("-Dcom.uber.tchannel.thrift_serializer.noPreferDirect: {}", !DIRECT_BUFFER_PREFERRED);
        }

        //heap is ok since we allocate once
        EMPTY_HEADER_BYTEBUF = ByteBufAllocator.DEFAULT.heapBuffer();
        CodecUtils.encodeHeaders(new HashMap<String, String>(), EMPTY_HEADER_BYTEBUF);
    }

    /**
     * Returns {@code true} if the platform has reliable low-level direct buffer access API and a user has not specified
     * {@code -Dcom.uber.tchannel.thrift_serializer.noPreferDirect} option.
     */
    public static boolean directBufferPreferred() {
        return DIRECT_BUFFER_PREFERRED;
    }

    @Override
    public @NotNull String decodeEndpoint(@NotNull ByteBuf arg1) {
        return arg1.toString(CharsetUtil.UTF_8);
    }

    @Override
    public @NotNull Map<String, String> decodeHeaders(@NotNull ByteBuf arg2) {
        return CodecUtils.decodeHeaders(arg2);
    }

    @Override
    public @Nullable <T> T decodeBody(@NotNull ByteBuf arg3, @NotNull Class<T> bodyType) {

        try {
            // Create a new instance of type 'T'
            T base = bodyType.getConstructor().newInstance();

            // Get byte[] from ByteBuf
            byte[] payloadBytes = new byte[arg3.readableBytes()];
            arg3.readBytes(payloadBytes);

            // Actually deserialize the payload
            TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
            deserializer.deserialize((TBase<?, ?>) base, payloadBytes);

            return base;
        } catch (
            NoSuchMethodException
                | InvocationTargetException
                | InstantiationException
                | IllegalAccessException
                | TException e
        ) {
            logger.error("Failed to decode body to {}", bodyType.getName(), e);
        }

        return null;

    }

    @Override
    public ByteBuf encodeEndpoint(@NotNull String method) {
        return Unpooled.wrappedBuffer(method.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public ByteBuf encodeHeaders(@NotNull Map<String, String> applicationHeaders) {
        if (applicationHeaders.isEmpty()) {
            return EMPTY_HEADER_BYTEBUF.copy();
        }
        boolean release = true;
        ByteBuf buf =
            DIRECT_BUFFER_PREFERRED ? ByteBufAllocator.DEFAULT.buffer() : ByteBufAllocator.DEFAULT.heapBuffer();
        try {
            CodecUtils.encodeHeaders(applicationHeaders, buf);
            release = false;
        } finally {
            if (release) {
                buf.release();
            }
        }
        return buf;
    }

    @Override
    public @Nullable ByteBuf encodeBody(@NotNull Object body) {
        try {
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            byte[] payloadBytes = serializer.serialize((TBase<?, ?>) body);
            return Unpooled.wrappedBuffer(payloadBytes);
        } catch (TException e) {
            logger.error("Failed to encode {} body", body.getClass().getName(), e);
        }
        return null;
    }

}
