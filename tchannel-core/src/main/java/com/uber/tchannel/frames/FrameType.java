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
package com.uber.tchannel.frames;

public enum FrameType {

    // First message on every connection must be init
    InitRequest((byte) 0x01),

    // Remote response to init req
    InitResponse((byte) 0x02),

    // RPC method request
    CallRequest((byte) 0x03),

    // RPC method response
    CallResponse((byte) 0x04),

    // RPC request continuation fragment
    CallRequestContinue((byte) 0x13),

    // RPC response continuation fragment
    CallResponseContinue((byte) 0x14),

    // CancelFrame an outstanding call req / forward req (no body)
    Cancel((byte) 0xc0),

    // ClaimFrame / cancel a redundant request
    Claim((byte) 0xc1),

    // Protocol level ping req (no body)
    PingRequest((byte) 0xd0),

    // PingFrame res (no body)
    PingResponse((byte) 0xd1),

    // Protocol level error.
    Error((byte) 0xff);

    private final byte type;

    FrameType(byte type) {
        this.type = type;
    }

    public static FrameType fromByte(byte value) {
        switch (value) {
            case (byte) 0x01:
                return InitRequest;
            case (byte) 0x02:
                return InitResponse;
            case (byte) 0x03:
                return CallRequest;
            case (byte) 0x04:
                return CallResponse;
            case (byte) 0x13:
                return CallRequestContinue;
            case (byte) 0x14:
                return CallResponseContinue;
            case (byte) 0xc0:
                return Cancel;
            case (byte) 0xc1:
                return Claim;
            case (byte) 0xd0:
                return PingRequest;
            case (byte) 0xd1:
                return PingResponse;
            case (byte) 0xff:
                return Error;
            default:
                return null;
        }
    }

    public byte byteValue() {
        return this.type;
    }

}
