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

package com.uber.tchannel.headers;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public enum ArgScheme {
    RAW("raw"),
    JSON("json"),
    HTTP("http"),
    THRIFT("thrift"),
    STREAMING_THRIFT("sthrift");

    private final @NotNull String scheme;

    ArgScheme(@NotNull String scheme) {
        this.scheme = scheme;
    }

    public static @Nullable ArgScheme toScheme(@Nullable String argScheme) {
        if (argScheme == null) {
            return null;
        }

        switch (argScheme) {
            case "raw":
                return RAW;
            case "json":
                return JSON;
            case "http":
                return HTTP;
            case "thrift":
                return THRIFT;
            case "sthrift":
                return STREAMING_THRIFT;
            default:
                return null;
        }
    }

    public static boolean isSupported(@Nullable ArgScheme scheme) {
        if (scheme == null) {
            return false;
        }

        switch (scheme) {
            case RAW:
            case JSON:
            case THRIFT:
                return true;
            default:
                return false;
        }
    }

    public @NotNull String getScheme() {
        return scheme;
    }

}
