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

package com.uber.tchannel.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class TChannelUtilities {

    private static final Logger logger = LoggerFactory.getLogger(TChannelUtilities.class);

    public static final ByteBuf emptyByteBuf = Unpooled.EMPTY_BUFFER;

    private TChannelUtilities() {}

    public static int scoreAddr(@NotNull NetworkInterface iface, @NotNull InetAddress addr) {
        int score = 0;

        if (addr instanceof Inet4Address) {
            score += 300;
        }

        try {
            if (!iface.isLoopback() && !addr.isLoopbackAddress()) {
                score += 100;
                if (iface.isUp()) {
                    score += 100;
                }
            }
        } catch (SocketException e) {
            logger.error("Unable to score IP {} of interface {}", addr, iface, e);
        }

        return score;
    }

    public static InetAddress getCurrentIp() {
        InetAddress bestAddr = null;

        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

            int bestScore = -1;
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface iface = networkInterfaces.nextElement();

                Enumeration<InetAddress> addrs = iface.getInetAddresses();

                while (addrs.hasMoreElements()) {
                    InetAddress addr = addrs.nextElement();

                    int score = scoreAddr(iface, addr);

                    // Prefer the latest match, instead of the earliest like
                    // the Go implementation. This is because the ordering of
                    // this output is reversed from Go.
                    if (score >= bestScore) {
                        bestScore = score;
                        bestAddr = addr;
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Problem getting local IP", e);
        }

        return bestAddr;
    }
}
