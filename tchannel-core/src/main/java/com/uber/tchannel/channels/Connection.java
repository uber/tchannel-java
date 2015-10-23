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
import com.uber.tchannel.api.errors.TChannelError;
import com.uber.tchannel.frames.InitFrame;
import io.netty.channel.Channel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;

/**
 * Connection represents a connection to a remote address
 */
public class Connection {
    public Direction direction = Direction.NONE;
    public ConnectionState state = ConnectionState.UNCONNECTED;

    private final Channel channel;
    private String remoteAddress = null;
    private TChannelError lastError = null;

    public Connection(Channel channel, Direction direction) {
        this.channel = channel;
        this.direction = direction;
        if (channel.isActive() && this.state == ConnectionState.UNCONNECTED) {
            this.state = ConnectionState.CONNECTED;
        }
    }

    public Channel channel() {
        return this.channel;
    }
    public TChannelError lastError() {
        return this.lastError;
    }

    public synchronized boolean satisfy(ConnectionState preferedState) {
        ConnectionState connState = this.state;
        if (connState == ConnectionState.DESTROYED) {
            return false;
        } else if (preferedState == null) {
            return true;
        } else if (connState == preferedState || connState == ConnectionState.IDENTIFIED) {
            return true;
        } else if (connState == ConnectionState.CONNECTED && preferedState == ConnectionState.UNCONNECTED) {
            return true;
        }

        return false;
    }

    public synchronized boolean satisfy(Direction preferedDirection) {
        Direction dir = this.direction;
        if (preferedDirection == null || preferedDirection == Direction.NONE) {
            return true;
        } else {
            return preferedDirection == dir;
        }
    }

    public synchronized void setState(ConnectionState state) {
        this.state = state;
        if (state == ConnectionState.IDENTIFIED || (
                state == ConnectionState.UNCONNECTED && this.lastError != null)) {
            this.notifyAll();
        }
    }

    public synchronized void setIndentified(Map<String, String> headers) {
        String hostPort = headers.get(InitFrame.HOST_PORT_KEY);
        if (hostPort == null) {
            // TODO: handle protocol error
            hostPort = "0.0.0.0:0";
        }

        this.remoteAddress = hostPort.trim();
        this.setState(ConnectionState.IDENTIFIED);
    }

    public synchronized void setIndentified(TChannelError error) {
        this.remoteAddress = null;
        this.lastError = error;
        this.setState(ConnectionState.UNCONNECTED);
    }

    public boolean isIndentified() {
        return state == ConnectionState.IDENTIFIED;
    }

    public synchronized boolean isEphemeral() {
        return this.remoteAddress.equals("0.0.0.0:0");
    }

    public String getRemoteAddress() {
        return this.remoteAddress;
    }

    public SocketAddress getRemoteAddressAsSocketAddress() {
        return hostPortToSocketAddress(this.remoteAddress);
    }

    public static String[] splitHostPort(String hostPort) {
        String[] strs = hostPort.split(":");
        if (strs.length != 2) {
            strs = new String[2];
            strs[0] = "0.0.0.0:";
            strs[1] = "0";
        }
        return strs;
    }

    public static SocketAddress hostPortToSocketAddress(String hostPort) {
        String[] strs = splitHostPort(hostPort);
        return new InetSocketAddress(strs[0], Integer.parseInt(strs[1]));
    }

    public synchronized boolean waitForIdentified(long timeout) {
        // TODO reap connections/peers on init timeout
        try {
            if (this.state != ConnectionState.IDENTIFIED) {
                this.lastError = null;
                this.wait(timeout);
            }
        } catch (InterruptedException ex) {
            // doesn't matter if we got interrupted here ...
        }

        return this.state == ConnectionState.IDENTIFIED;
    }

    public synchronized void close() throws InterruptedException {
        channel.close().sync();
        this.state = ConnectionState.DESTROYED;
    }

    public enum Direction {
        NONE,
        IN,
        OUT
    }
}
