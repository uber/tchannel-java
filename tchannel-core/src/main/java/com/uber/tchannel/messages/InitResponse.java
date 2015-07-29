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

import java.util.Map;

/**
 * Similar to {@link InitRequest}. The initiator requests a version number, and the server responds with the
 * actual version that will be used for the rest of this connection. The header name/values are the same,
 * but identify the server.
 */
public class InitResponse implements InitMessage, Message {

    private final long id;
    private final int version;
    private final Map<String, String> headers;

    public InitResponse(long id, int version, Map<String, String> headers) {
        this.id = id;
        this.version = version;
        this.headers = headers;
    }

    public int getVersion() {
        return version;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public long getId() {
        return this.id;
    }

    public MessageType getMessageType() {
        return MessageType.InitResponse;
    }

    public String getHostPort() {
        return this.headers.get(HOST_PORT_KEY);
    }

    public String getProcessName() {
        return this.headers.get(PROCESS_NAME_KEY);
    }

}
