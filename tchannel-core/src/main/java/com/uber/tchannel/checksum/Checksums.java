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

import java.util.zip.Adler32;
import java.util.zip.CRC32;

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

        switch (msg.getChecksumType()) {

            case Adler32:
                Adler32 adler32 = new Adler32();
                adler32.update((int) digestSeed);
                adler32.update(msg.getPayload().nioBuffer());
                return adler32.getValue();
            case CRC32C:
                CRC32 crc32 = new CRC32();
                crc32.update((int) digestSeed);
                crc32.update(msg.getPayload().nioBuffer());
                return crc32.getValue();
            case FarmhashFingerPrint32:
            case NoChecksum:
            default:
                return 0;
        }

    }

}
