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

package com.uber.tchannel.errors;

public enum ErrorType {
    Invalid((byte) 0x00),
    Timeout((byte) 0x01),
    Cancelled((byte) 0x02),
    Busy((byte) 0x03),
    Declined((byte) 0x04),
    UnexpectedError((byte) 0x05),
    BadRequest((byte) 0x06),
    NetworkError((byte) 0x07),
    Unhealthy((byte) 0x08),
    FatalProtocolError((byte) 0xff);

    private final byte code;

    ErrorType(byte code) {
        this.code = code;
    }

    public static ErrorType fromByte(byte value) {
        switch (value) {
            case (byte) 0x00:
                return Invalid;
            case (byte) 0x01:
                return Timeout;
            case (byte) 0x02:
                return Cancelled;
            case (byte) 0x03:
                return Busy;
            case (byte) 0x04:
                return Declined;
            case (byte) 0x05:
                return UnexpectedError;
            case (byte) 0x06:
                return BadRequest;
            case (byte) 0x07:
                return NetworkError;
            case (byte) 0x08:
                return Unhealthy;
            case (byte) 0xff:
                return FatalProtocolError;
            default:
                return null;
        }
    }

    public byte byteValue() {
        return this.code;
    }
}
