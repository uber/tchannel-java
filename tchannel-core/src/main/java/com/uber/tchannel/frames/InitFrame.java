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

import java.util.Map;

public abstract class InitFrame extends Frame {

    public static final int DEFAULT_VERSION = 2;
    public static final String HOST_PORT_KEY = "host_port";
    public static final String PROCESS_NAME_KEY = "process_name";

    /**
     * version is a 16 bit number. The currently specified protocol version is 2.
     * If new versions are required, this is where a common version can be negotiated.
     *
     * @return 16-bit unsigned integer representing the specified protocol version.
     */
    public abstract int getVersion();

    /**
     * There are a variable number of key/value pairs. For version 2, the following are required:
     * <p>
     * host_port: where this process can be reached. format: address:port
     * process_name: additional identifier for this instance, used for logging. format: arbitrary string
     *
     * @return Map of headers
     */
    public abstract Map<String, String> getHeaders();

    /**
     * Where the sending process can be reached.
     * <p>
     * Key: host_port
     * Format: address:port
     * Protocol Description: where this process can be reached
     *
     * @return the `host_port` key for the `headers` member.
     */
    public abstract String getHostPort();

    /**
     * An additional process identifier for the sending process, used for logging.
     * <p>
     * Key: process_name
     * Format: arbitrary string
     * Protocol Description: additional identifier for this instance, used for logging
     *
     * @return the `process_name` key for the `headers` member.
     */
    public abstract String getProcessName();

}
