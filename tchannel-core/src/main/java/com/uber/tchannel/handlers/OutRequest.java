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
import com.uber.tchannel.tracing.TracingContext;
import io.netty.channel.ChannelFuture;
import io.netty.util.Timeout;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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

    private final @NotNull SubChannel subChannel;
    private final @NotNull Request request;
    private final @NotNull TFuture<V> future;
    private final @NotNull Set<SocketAddress> usedPeers = new HashSet<>();
    private final @NotNull AtomicInteger retryCount = new AtomicInteger(0);

    private int retryLimit = 0;
    private @Nullable Timeout timeout = null;
    private @Nullable ChannelFuture channelFuture = null;
    private @Nullable ErrorResponse lastError = null;

    public OutRequest(
        @NotNull SubChannel subChannel,
        @NotNull Request request,
        @Nullable TracingContext tracingContext
    ) {
        this.subChannel = subChannel;
        this.request = request;
        this.future = TFuture.create(request.getArgScheme(), tracingContext);
        this.retryLimit = request.getRetryLimit();
    }

    /** @deprecated Use {@link #OutRequest(SubChannel, Request, TracingContext)}. */
    @Deprecated
    public OutRequest(@NotNull SubChannel subChannel, @NotNull Request request) {
        this(subChannel, request, null);
    }

    public @NotNull Request getRequest() {
        return request;
    }

    public @NotNull TFuture<V> getFuture() {
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

        return shouldRetryOnError();
    }

    public @Nullable Timeout getTimeout() {
        return timeout;
    }

    public void setTimeout(@Nullable Timeout timeout) {
        this.timeout = timeout;
    }

    public @Nullable ChannelFuture getChannelFuture() {
        return channelFuture;
    }

    public void setChannelFuture(@NotNull ChannelFuture channelFuture) {
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

        if (lastError != null) {
            lastError.touch("OutRequest.release()");
        }
    }

    public boolean isUsedPeer(SocketAddress address) {
        return this.usedPeers.contains(address);
    }

    public void setUsedPeer(SocketAddress address) {
        this.usedPeers.add(address);
    }

    public @Nullable ErrorResponse getLastError() {
        return lastError;
    }

    public void setLastError(@NotNull ErrorResponse lastError) {
        if (this.lastError != null) {
            this.lastError.touch("Evicted previous last error OutRequest.setLastError(...)");
        }
        this.lastError = lastError;
    }

    public void setLastError(ErrorType errorType, Throwable throwable) {
        setLastError(new ErrorResponse(request.getId(), errorType, throwable));
    }

    public void setLastError(ErrorType errorType, String message) {
        setLastError(new ErrorResponse(request.getId(), errorType, message));
    }

    public void setFuture(@NotNull Response response) {
        release();
        setResponseFuture(request.getArgScheme(), response);
//        if (lastError != null) {
//            lastError.release();
//        }
        if (lastError != null) {
            lastError.touch("OutRequest.setFuture(...) left last error non-released");
        }
    }

    public void setFuture() {
        setFuture(Response.build(request.getArgScheme(), getLastError()));
    }

    public void handleResponse(@NotNull ResponseMessage response) {
        if (response != null) {
            response.touch("OutRequest.handleResponse(...)");
        }
        if (!response.isError()) {
            setFuture((Response) response);
            return;
        }

        if (response != null) {
            response.touch("OutRequest.handleResponse-setLastErrors(...)");
        }
        setLastError((ErrorResponse)response);

        // reset the read index of args for retries
        request.reset();
        subChannel.sendOutRequest(this);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected void setResponseFuture(ArgScheme argScheme, Response response) {
        if (response != null) {
            String requestString = request == null ? "<unknown_request>" : this.request.toString();
            response.touch("OutRequest.setResponseFuture(" + argScheme + ", " + requestString + ")");
        }
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
