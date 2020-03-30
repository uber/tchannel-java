package com.uber.tchannel.handlers;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.AsyncRequestHandler;
import com.uber.tchannel.errors.BadRequestError;
import com.uber.tchannel.errors.BusyError;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

public class RequestRouterTest {

    private static TChannel tchannel;

    private static SubChannel subChannel;

    private static ThrowingAsyncHandler handler;

    @BeforeClass
    public static void setUp() throws Exception {
        handler = new ThrowingAsyncHandler();

        tchannel = new TChannel.Builder("tchannel-name")
            .setServerHost(InetAddress.getByName(null))
            .build();

        subChannel = tchannel.makeSubChannel("service")
            .register("endpoint", handler);

        tchannel.listen();
    }

    @Test
    public void returnUnexpectedErrorOnThrowable() throws Exception {
        handler.setThrowable(new RuntimeException("unexpected error"));

        RawRequest req = new RawRequest.Builder("service", "endpoint").build();

        TFuture<RawResponse> responseTFuture = subChannel.send(
            req,
            tchannel.getHost(),
            tchannel.getListeningPort()
        );

        RawResponse response = responseTFuture.get();

        assertThat(
            "Failed future must result in an error",
            response.getError(),
            notNullValue()
        );

        assertThat(
            "Throwable must be mapped to ErrorType.UnexpectedError",
            response.getError().getErrorType(),
            equalTo(ErrorType.UnexpectedError)
        );

        assertThat(
            "ProtocolError must be mapped to its message",
            response.getError().getMessage(),
            equalTo("Failed to handle the request: unexpected error")
        );
    }

    @Test
    public void propagateErrorCodeOnProtocolError() throws Exception {
        handler.setThrowable(new BusyError("busy", null, 0));

        RawRequest req = new RawRequest.Builder("service", "endpoint").build();

        TFuture<RawResponse> responseTFuture = subChannel.send(
            req,
            tchannel.getHost(),
            tchannel.getListeningPort()
        );

        RawResponse response = responseTFuture.get();

        assertThat(
            "Failed future must result in an error",
            response.getError(),
            notNullValue()
        );

        assertThat(
            "ProtocolError must be mapped to its ErrorType",
            response.getError().getErrorType(),
            equalTo(ErrorType.Busy)
        );

        assertThat(
            "ProtocolError must be mapped to its message",
            response.getError().getMessage(),
            equalTo("busy")
        );
    }

    @Test
    public void propagateErrorCodeOnProtocolErrorCause() throws Exception {
        handler.setThrowable(new RuntimeException(
            new ExecutionException(new BadRequestError("bad request", null, 0))
        ));

        RawRequest req = new RawRequest.Builder("service", "endpoint").build();

        TFuture<RawResponse> responseTFuture = subChannel.send(
            req,
            tchannel.getHost(),
            tchannel.getListeningPort()
        );

        RawResponse response = responseTFuture.get();

        assertThat(
            "Failed future must result in an error",
            response.getError(),
            notNullValue()
        );

        assertThat(
            "ProtocolError must be mapped to its ErrorType",
            response.getError().getErrorType(),
            equalTo(ErrorType.BadRequest)
        );

        assertThat(
            "ProtocolError must be mapped to its message",
            response.getError().getMessage(),
            equalTo("bad request")
        );
    }

    private static class ThrowingAsyncHandler implements AsyncRequestHandler {

        private Throwable throwable;

        @Override
        public ListenableFuture<? extends Response> handleAsync(Request request) {
            return Futures.immediateFailedFuture(throwable);
        }

        @Override
        public Response handle(Request request) {
            throw new UnsupportedOperationException();
        }

        public void setThrowable(Throwable throwable) {
            this.throwable = throwable;
        }
    }
}
