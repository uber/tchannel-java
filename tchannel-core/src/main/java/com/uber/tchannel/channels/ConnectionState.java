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

package com.uber.tchannel.channels;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public enum ConnectionState {

    UNCONNECTED("unconnected"),
    CONNECTED("connected"),
    IDENTIFIED("identified"),
    DESTROYED("destroyed");

    private final @NotNull String state;

    ConnectionState(@NotNull String state) {
        this.state = state;
    }

    public static @Nullable ConnectionState toState(@Nullable String state) {
        if (state == null) {
            return null;
        }

        switch (state) {
            case "unconnected":
                return UNCONNECTED;
            case "connected":
                return CONNECTED;
            case "identified":
                return IDENTIFIED;
            case "destroyed":
                return DESTROYED;
            default:
                return null;
        }
    }

    public static boolean isConnected(ConnectionState state) {
        return state == IDENTIFIED || state == CONNECTED;
    }

    public static boolean isConnected(@Nullable Connection conn) {
        return conn != null && isConnected(conn.state);

    }

    public @NotNull String getState() {
        return state;
    }

}
