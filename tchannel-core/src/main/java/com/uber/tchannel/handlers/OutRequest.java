package com.uber.tchannel.handlers;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.headers.ArgScheme;
import com.uber.tchannel.messages.ErrorResponse;
import com.uber.tchannel.messages.JsonResponse;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import com.uber.tchannel.messages.ResponseMessage;
import com.uber.tchannel.messages.ThriftResponse;
import io.netty.channel.ChannelFuture;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The logic unit for managing out requests
 */
public final class OutRequest<V extends Response> {
    private static final Logger logger = LoggerFactory.getLogger(OutRequest.class);

    private final SubChannel subChannel;
    private final Request request;
    private final TFuture<V> future;
    private final Set<SocketAddress> usedPeers = new HashSet<>();

    private final AtomicInteger retryCount = new AtomicInteger(0);
    private int retryLimit = 0;
    private Timeout timeout = null;
    private ChannelFuture channelFuture = null;

    private ErrorResponse lastError = null;

    public OutRequest(SubChannel subChannel, Request request) {
        this.subChannel = subChannel;
        this.request = request;
        this.future = TFuture.<V>create(request.getArgScheme());
        this.retryLimit = request.getRetryLimit();
    }

    public Request getRequest() {
        return request;
    }

    public TFuture<V> getFuture() {
        return future;
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public void disableRetry() {
        retryLimit = 0;
        retryCount.set(1);
    }

    public boolean shouldRetry() {
        int count = retryCount.getAndIncrement();
        if (count > retryLimit) {
            return false;
        }

        // if it is the first attempt
        if (count == 0) {
            return true;
        }

        if (!shouldRetryOnError()) {
            return false;
        }

        return true;
    }

    public Timeout getTimeout() {
        return timeout;
    }

    public void setTimeout(Timeout timeout) {
        this.timeout = timeout;
    }

    public ChannelFuture getChannelFuture() {
        return channelFuture;
    }

    public void setChannelFuture(ChannelFuture channelFuture) {
        this.channelFuture = channelFuture;
    }

    public void flushWrite() {
        if (channelFuture == null) {
            return;
        }

        try {
            this.channelFuture.sync();
        } catch (InterruptedException ie) {
            // set interrupt flag
            Thread.currentThread().interrupt();
            logger.warn("flushWrite got interrupted.", ie);
        }
    }

    public void release() {
        if (timeout != null) {
            timeout.cancel();
        }

        request.release();
    }

    public boolean isUsedPeer(SocketAddress address) {
        return this.usedPeers.contains(address);
    }

    public void setUsedPeer(SocketAddress address) {
        this.usedPeers.add(address);
    }

    public ErrorResponse getLastError() {
        return lastError;
    }

    public void setLastError(ErrorResponse lastError) {
        this.lastError = lastError;
    }

    public void setLastError(ErrorType errorType, Throwable throwable) {
        setLastError(new ErrorResponse(
            request.getId(),
            errorType,
            throwable));
    }

    public void setLastError(ErrorType errorType, String message) {
        setLastError(new ErrorResponse(
            request.getId(),
            errorType,
            message));
    }

    public void setFuture(Response response) {
        release();
        setResponseFuture(request.getArgScheme(), response);
    }

    public void setFuture() {
        Response response = Response.build(request.getArgScheme(), getLastError());
        setFuture(response);
    }

    public void handleResponse(ResponseMessage response) {
        if (!response.isError()) {
            setFuture((Response) response);
            return;
        }

        setLastError((ErrorResponse)response);

        // reset the read index of args for retries
        request.reset();
        subChannel.sendOutRequest(this);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void setResponseFuture(ArgScheme argScheme, Response response) {
        switch (argScheme) {
            case RAW:
                ((TFuture<RawResponse>)future).set((RawResponse) response);
                break;
            case JSON:
                ((TFuture<JsonResponse>)future).set((JsonResponse) response);
                break;
            case THRIFT:
                ((TFuture<ThriftResponse>)future).set((ThriftResponse) response);
                break;
            default:
                logger.error("unsupported arg scheme: {}", argScheme);
                ((TFuture<RawResponse>)future).set((RawResponse) response);
                break;
        }
    }

    protected boolean shouldRetryOnError() {
        if (lastError == null) {
            return false;
        }

        String flags = request.getRetryFlags();
        if (flags.contains("n")) {
            return false;
        }

        ErrorType errorType = lastError.getErrorType();

        switch (errorType) {
            case BadRequest:
            case Cancelled:
            case Unhealthy:
                return false;

            case Busy:
            case Declined:
                return true;

            case Timeout:
                return flags.contains("t");

            case NetworkError:
            case FatalProtocolError:
            case UnexpectedError:
                 return flags.contains("c");

            default:
                return false;
        }
    }
}
