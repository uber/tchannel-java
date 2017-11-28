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
import com.uber.tchannel.handlers.ResponseRouter;
import io.netty.channel.Channel;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;

/**
 * Connection represents a connection to a remote address
 */
public class Connection {

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    private static final String EPHEMERAL = "0.0.0.0:0";

    public Direction direction = Direction.NONE;
    public ConnectionState state = ConnectionState.UNCONNECTED;

    private Peer peer;
    private final @NotNull Channel channel;
    private @Nullable String remoteAddress = null;
    private @Nullable TChannelError lastError = null;
    final protected @NotNull Object lock;

    public Connection(Peer peer, @NotNull Channel channel, Direction direction) {
        this.peer = peer;
        this.channel = channel;
        this.direction = direction;
        if (channel.isActive() && this.state == ConnectionState.UNCONNECTED) {
            this.state = ConnectionState.CONNECTED;
        }
        lock = new Object();
    }

    public @NotNull Channel channel() {
        return this.channel;
    }

    public @Nullable TChannelError lastError() {
        synchronized (lock) {
            return this.lastError;
        }
    }

    public boolean satisfy(@Nullable ConnectionState preferredState) {
        synchronized (lock) {
            ConnectionState connState = this.state;
            if (connState == ConnectionState.DESTROYED) {
                return false;
            } else if (preferredState == null) {
                return true;
            } else if (connState == preferredState || connState == ConnectionState.IDENTIFIED) {
                return true;
            } else if (connState == ConnectionState.CONNECTED && preferredState == ConnectionState.UNCONNECTED) {
                return true;
            } else {
                return false;
            }
        }
    }

    public boolean satisfy(@Nullable Direction preferredDirection) {
        synchronized (lock) {
            return preferredDirection == null || preferredDirection == Direction.NONE || preferredDirection == direction;
        }
    }

    public void setState(ConnectionState state) {
        synchronized (lock) {
            this.state = state;
            if (state == ConnectionState.IDENTIFIED || (state == ConnectionState.UNCONNECTED && this.lastError != null)) {
                lock.notifyAll();
            }
        }
    }

    public void setIdentified(@NotNull Map<String, String> headers) {
        synchronized (lock) {
            String hostPort = headers.get(InitFrame.HOST_PORT_KEY);
            // TODO: handle protocol error
            this.remoteAddress = hostPort == null ? EPHEMERAL : hostPort.trim();
            this.setState(ConnectionState.IDENTIFIED);
        }
    }

    /** @deprecated typo - use {@link #setIdentified(Map)} */
    @Deprecated
    public void setIndentified(@NotNull Map<String, String> headers) {
            setIdentified(headers);
    }

    public synchronized void setIdentified(TChannelError error) {
        synchronized (lock) {
            this.remoteAddress = null;
            this.lastError = error;
            this.setState(ConnectionState.UNCONNECTED);
        }
    }

    /** @deprecated typo - use {@link #setIdentified(TChannelError)} */
    @Deprecated
    public void setIndentified(TChannelError error) {
        setIdentified(error);
    }

    public boolean isIdentified() {
        return state == ConnectionState.IDENTIFIED;
    }

    /** @deprecated typo - use {@link #isIdentified} */
    @Deprecated
    public boolean isIndentified() {
        return isIdentified();
    }

    public boolean isEphemeral() {
        synchronized (lock) {
            return EPHEMERAL.equals(this.remoteAddress);
        }
    }

    public @Nullable String getRemoteAddress() {
        synchronized (lock) {
            return this.remoteAddress;
        }
    }

    public @NotNull SocketAddress getRemoteAddressAsSocketAddress() {
        synchronized (lock) {
            return hostPortToSocketAddress(this.remoteAddress);
        }
    }

    public static @NotNull String[] splitHostPort(@NotNull String hostPort) {
        String[] strs = hostPort.split(":");
        if (strs.length != 2) {
            strs = new String[2];
            strs[0] = "0.0.0.0:"; // FIXME check the trailing colon should indeed be here
            strs[1] = "0";
        }
        return strs;
    }

    public static @NotNull SocketAddress hostPortToSocketAddress(@NotNull String hostPort) {
        String[] strs = splitHostPort(hostPort);
        return new InetSocketAddress(strs[0], Integer.parseInt(strs[1]));
    }

    public boolean waitForIdentified(long timeout) {
        synchronized (lock) {
            // TODO reap connections/peers on init timeout
            try {
                if (this.state != ConnectionState.IDENTIFIED) {
                    this.lastError = null;
                    lock.wait(timeout);
                }
            } catch (InterruptedException ex) {
                // doesn't matter if we got interrupted here ...
                // set interrupt flag
                Thread.currentThread().interrupt();
                logger.warn("wait for identified is interrupted.", ex);
            }

            boolean result = this.state == ConnectionState.IDENTIFIED;
            if (!result) {
                // reset the connection if it failed to identify
                this.clean();
            }

            return result;
        }
    }

    public void close() {
        synchronized (lock) {
            ResponseRouter responseRouter = channel.pipeline().get(ResponseRouter.class);
            if (responseRouter != null) {
                responseRouter.clean();
            }

            channel.close();
            this.state = ConnectionState.DESTROYED;
        }
    }

    public void clean() {
        this.close();
        this.peer.remove(this);
    }

    public Peer getPeer() {
        return peer;
    }

    public void setPeer(Peer peer) {
        this.peer = peer;
    }

    public enum Direction {
        NONE,
        IN,
        OUT
    }

}
