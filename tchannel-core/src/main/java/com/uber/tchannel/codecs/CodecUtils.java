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
package com.uber.tchannel.codecs;

import com.uber.tchannel.checksum.ChecksumType;
import com.uber.tchannel.tracing.Trace;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

public final class CodecUtils {

    public static int decodeChecksum(ChecksumType checksumType, ByteBuf buffer) {
        switch (checksumType) {
            case Adler32:
            case FarmhashFingerPrint32:
            case CRC32C:
                return buffer.readInt();
            case NoChecksum:
            default:
                return 0;
        }
    }

    public static void encodeChecksum(int checksum, ChecksumType checksumType, ByteBuf buffer) {
        switch (checksumType) {
            case Adler32:
            case FarmhashFingerPrint32:
            case CRC32C:
                buffer.writeInt(checksum);
                break;
            case NoChecksum:
            default:
                break;
        }
    }

    public static String decodeString(ByteBuf buffer) {
        int valueLength = buffer.readUnsignedShort();
        byte[] valueBytes = new byte[valueLength];
        buffer.readBytes(valueBytes);
        return new String(valueBytes);
    }

    public static void encodeString(String value, ByteBuf buffer) {
        byte[] raw = value.getBytes();
        buffer.writeShort(raw.length);
        buffer.writeBytes(raw);
    }

    public static String decodeSmallString(ByteBuf buffer) {
        int valueLength = buffer.readUnsignedByte();
        byte[] valueBytes = new byte[valueLength];
        buffer.readBytes(valueBytes);
        return new String(valueBytes);
    }

    public static void encodeSmallString(String value, ByteBuf buffer) {
        byte[] raw = value.getBytes();
        buffer.writeByte(raw.length);
        buffer.writeBytes(raw);
    }

    public static Map<String, String> decodeHeaders(ByteBuf buffer) {

        int numHeaders = buffer.readUnsignedShort();
        Map<String, String> headers = Maps.newHashMapWithExpectedSize(numHeaders);

        for (int i = 0; i < numHeaders; i++) {
            String key = CodecUtils.decodeString(buffer);
            String value = CodecUtils.decodeString(buffer);
            headers.put(key, value);

        }

        return headers;

    }

    public static void encodeHeaders(Map<String, String> headers, ByteBuf buffer) {

        buffer.writeShort(headers.size());

        for (Map.Entry<String, String> header : headers.entrySet()) {
            CodecUtils.encodeString(header.getKey(), buffer);
            CodecUtils.encodeString(header.getValue(), buffer);
        }

    }

    public static Map<String, String> decodeSmallHeaders(ByteBuf buffer) {

        short numHeaders = buffer.readUnsignedByte();
        Map<String, String> headers = Maps.newHashMapWithExpectedSize(numHeaders);

        for (int i = 0; i < numHeaders; i++) {
            String key = CodecUtils.decodeSmallString(buffer);
            String value = CodecUtils.decodeSmallString(buffer);
            headers.put(key, value);
        }

        return headers;

    }

    public static void encodeSmallHeaders(Map<String, String> headers, ByteBuf buffer) {

        buffer.writeByte(headers.size());

        for (Map.Entry<String, String> header : headers.entrySet()) {
            CodecUtils.encodeSmallString(header.getKey(), buffer);
            CodecUtils.encodeSmallString(header.getValue(), buffer);
        }
    }

    public static Trace decodeTrace(ByteBuf buffer) {
        long spanId = buffer.readLong();
        long parentId = buffer.readLong();
        long traceId = buffer.readLong();
        byte traceFlags = buffer.readByte();

        return new Trace(spanId, parentId, traceId, traceFlags);
    }

    public static void encodeTrace(Trace trace, ByteBuf buffer) {
        buffer.writeLong(trace.spanId)
                .writeLong(trace.parentId)
                .writeLong(trace.traceId)
                .writeByte(trace.traceFlags);
    }

    public static int writeArg(ByteBufAllocator allocator, ByteBuf arg, int writableBytes, List<ByteBuf> bufs) {
        if (writableBytes <= TFrame.FRAME_SIZE_LENGTH) {
            throw new UnsupportedOperationException("writableBytes must be larger than " + TFrame.FRAME_SIZE_LENGTH);
        }

        int readableBytes = arg.readableBytes();
        int headerSize = TFrame.FRAME_SIZE_LENGTH;
        int chunkLength = Math.min(readableBytes + headerSize, writableBytes);
        ByteBuf sizeBuf = allocator.buffer(TFrame.FRAME_SIZE_LENGTH);
        bufs.add(sizeBuf);

        // Write the size of the `arg`
        sizeBuf.writeShort(chunkLength - headerSize);
        if (readableBytes == 0) {
            return TFrame.FRAME_SIZE_LENGTH;
        } else {
            bufs.add(arg.readSlice(chunkLength - headerSize).retain());
            return chunkLength;
        }
    }

    public static ByteBuf writeArgs(ByteBufAllocator allocator,
                                    ByteBuf header,
                                    List<ByteBuf> args) {
        int writableBytes = TFrame.MAX_FRAME_PAYLOAD_LENGTH - header.readableBytes();
        List<ByteBuf> bufs = new ArrayList<>(7);
        bufs.add(header);

        while (!args.isEmpty()) {
            ByteBuf arg = args.get(0);
            int len = writeArg(allocator, arg, writableBytes, bufs);
            writableBytes -= len;
            if (writableBytes <= TFrame.FRAME_SIZE_LENGTH) {
                break;
            }

            if (arg.readableBytes() == 0) {
                args.remove(0);
            }
        }

        CompositeByteBuf comp = allocator.compositeBuffer();
        comp.addComponents(bufs);
        comp.writerIndex(TFrame.MAX_FRAME_PAYLOAD_LENGTH - writableBytes);

        return comp;
    }

    public static ByteBuf writeArgCopy(ByteBufAllocator allocator, ByteBuf payload, ByteBuf arg, int writableBytes) {
        if (writableBytes <= TFrame.FRAME_SIZE_LENGTH) {
            throw new UnsupportedOperationException("writableBytes must be larger than " + TFrame.FRAME_SIZE_LENGTH);
        }

        int readableBytes = arg.readableBytes();
        int headerSize = TFrame.FRAME_SIZE_LENGTH;
        int chunkLength = Math.min(readableBytes + headerSize, writableBytes);

        // Write the size of the `arg`
        payload.writeShort(chunkLength - headerSize);
        if (readableBytes == 0) {
            return payload;
        } else {
            return payload.writeBytes(arg, chunkLength - headerSize);
        }
    }

    public static ByteBuf writeArgsCopy(ByteBufAllocator allocator,
                                    ByteBuf header,
                                    List<ByteBuf> args) {
        ByteBuf payload = allocator.buffer(header.readableBytes(), TFrame.MAX_FRAME_PAYLOAD_LENGTH);
        payload.writeBytes(header);
        header.release();
        int writableBytes = TFrame.MAX_FRAME_PAYLOAD_LENGTH - payload.readableBytes();

        while (!args.isEmpty()) {
            ByteBuf arg = args.get(0);
            writeArgCopy(allocator, payload, arg, writableBytes);
            writableBytes = TFrame.MAX_FRAME_PAYLOAD_LENGTH - payload.readableBytes();
            if (writableBytes <= TFrame.FRAME_SIZE_LENGTH) {
                break;
            }

            if (arg.readableBytes() == 0) {
                args.remove(0);
            }
        }

        return payload;
    }

    public static ByteBuf compose(ByteBuf first, ByteBuf second) {
        if (first == Unpooled.EMPTY_BUFFER) {
            return second;
        } else if (second == Unpooled.EMPTY_BUFFER) {
            return first;
        } else {
            return Unpooled.wrappedBuffer(first, second);
        }
    }

    public static ByteBuf readArg(ByteBuf buffer) {
        if (buffer.readableBytes() < TFrame.FRAME_SIZE_LENGTH) {
            return null;
        }

        int len = buffer.readUnsignedShort();
        if (len > buffer.readableBytes()) {
            throw new UnsupportedOperationException("wrong read index for args");
        } else if (len == 0) {
            return Unpooled.EMPTY_BUFFER;
        }

        /* Read a slice, retain a copy */
        ByteBuf arg = buffer.readSlice(len);
        arg.retain();

        return arg;
    }

    public static void readArgs(List<ByteBuf> args, ByteBuf buffer) {

        ByteBuf arg = null;
        if (args.isEmpty()) {
            args.add(Unpooled.EMPTY_BUFFER);
        }

        boolean first = true;
        while (true) {
            arg = readArg(buffer);
            if (arg == null) {
                return;
            } else if (first) {
                first = false;
                ByteBuf prev = args.get(args.size() - 1);
                args.set(args.size() - 1, compose(prev, arg));
            } else {
                args.add(arg);
            }
        }
    }
}
