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
package com.uber.tchannel.checksum;

import com.uber.tchannel.messages.CallMessage;
import io.netty.buffer.ByteBuf;

import java.util.zip.Adler32;

public final class Checksums {
    public static boolean verifyChecksum(CallMessage msg) {
        return (calculateChecksum(msg) == msg.getChecksum());
    }

    public static boolean verifyExistingChecksum(CallMessage msg, long checksum) {
        return (msg.getChecksum() == checksum);
    }

    public static long calculateChecksum(CallMessage msg) {
        return calculateChecksum(msg, 0L);
    }

    public static long calculateChecksum(CallMessage msg, long digestSeed) {

        // TODO: this is bad
        ByteBuf payloadCopy = msg.getPayload().slice();
        byte[] payloadBytes = new byte[msg.getPayloadSize()];
        payloadCopy.readBytes(payloadBytes);

        switch (msg.getChecksumType()) {

            case Adler32:
                Adler32 f = new Adler32();
                f.update((int) digestSeed);
                f.update(payloadBytes);
                return f.getValue();
            case FarmhashFingerPrint32:
            case NoChecksum:
            case CRC32C:
            default:
                return 0;
        }

    }

}
