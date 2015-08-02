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

import java.util.HashMap;
import java.util.Map;

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
        Map<String, String> headers = new HashMap<String, String>(numHeaders);

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
        Map<String, String> headers = new HashMap<String, String>(numHeaders);

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

}
