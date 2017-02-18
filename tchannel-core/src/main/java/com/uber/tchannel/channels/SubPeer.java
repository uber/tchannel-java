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

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.handlers.OutRequest;

import java.net.SocketAddress;

public class SubPeer {
    private static final double SCORE_UNCONNECTED = 1.0;
    private static final double SCORE_CONNECTED = 2.0;
    private static final double SCORE_IDENTIFIED = 3.0;
    private static final double SCORE_PREFERRED_DIRECTION = 4.0;

    private static final double SCORE_FUZZ = 0.5;

    private final SocketAddress remoteAddress;
    private final PeerManager peerManager;

    private double score = 0;
    private Connection connection = null;
    private Connection.Direction direction = Connection.Direction.NONE;

    public SubPeer(SocketAddress remoteAddress, SubChannel subChannel) {
        this.remoteAddress = remoteAddress;
        this.peerManager = subChannel.getPeerManager();
        this.direction = subChannel.getPreferredDirection();
    }

    public SubPeer setPreference(Connection.Direction direction) {
        this.direction = direction;
        return this;
    }

    public Peer getPeer() {
        return peerManager.getPeer(remoteAddress);
    }

    public boolean updateScore(OutRequest outRequest) {
        if (outRequest.isUsedPeer(remoteAddress)) {
            // if we have used this peer, it should be given a lower score
            score = SCORE_UNCONNECTED - 0.1;
            return false;
        }

        Peer peer = getPeer();
        if (peer == null) {
            score = SCORE_UNCONNECTED;
            return false;
        }

        connection = peer.getConnection(ConnectionState.IDENTIFIED, direction);
        if (!ConnectionState.isConnected(connection)) {
            score = SCORE_UNCONNECTED;
        } else if (!connection.isIndentified()) {
            score = SCORE_CONNECTED;
        } else if (direction != Connection.Direction.NONE && direction != connection.direction) {
            score = SCORE_IDENTIFIED;
        } else {
            score = SCORE_PREFERRED_DIRECTION;
            return true;
        }

        return false;
    }

    public Connection getPreferredConnection() {
        return connection;
    }

    public double getScore() {
        return score;
    }

    public Connection connectTo() {
        return peerManager.connectTo(remoteAddress);
    }

    public SocketAddress getRemoteAddress() {
        return this.remoteAddress;
    }
}
