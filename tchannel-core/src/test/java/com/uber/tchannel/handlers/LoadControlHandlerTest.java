package com.uber.tchannel.handlers;

import static org.junit.Assert.assertEquals;

import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TChannel.Builder;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import java.net.InetAddress;
import java.util.concurrent.Semaphore;
import org.junit.Test;

public class LoadControlHandlerTest {

    @Test(expected = IllegalArgumentException.class)
    public void invariant_lowLessThanZero() {
        new Builder("server")
            .setChildLoadControl(-1, 10)
            .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void invariant_highEqualToLow() {
        new TChannel.Builder("server")
            .setChildLoadControl(10, 10)
            .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void invariant_highLessThanLow() {
        new TChannel.Builder("server")
            .setChildLoadControl(10, 5)
            .build();
    }

    /**
     * Send two requests in parallel.
     *
     * One of the requests will not be resolved, because it triggers the high water mark.
     *
     * The request that _does_ resolve, will not trigger the low water mark.
     *
     * So the other request will timeout.
     */
    @Test
    public void testHighWaterMark() throws Exception {

        try (Scenario scenario = new Scenario(0, 2)) {
            TFuture<RawResponse> future1 = scenario.sendRequest(100);
            TFuture<RawResponse> future2 = scenario.sendRequest(100);

            scenario.releaseResponses(2);

            int success = 0;

            if (ResponseCode.OK.equals(future1.get().getResponseCode())) {
                success += 1;
                assertEquals(ErrorType.Timeout, future2.get().getError().getErrorType());
            }

            if (ResponseCode.OK.equals(future2.get().getResponseCode())) {
                success += 1;
                assertEquals(ErrorType.Timeout, future1.get().getError().getErrorType());
            }

            assertEquals(1, success);
        }
    }

    /**
     * Send two requests in parallel.
     *
     * One of the requests will trigger the water mark. It will not be read.
     *
     * The other request will be resolved, and will trigger the low water mark.
     *
     * The paused request is resumed.
     *
     * All requests are resolved.
     */
    @Test
    public void testLowWaterMark() throws Exception {

        try (Scenario scenario = new Scenario(1, 2)) {
            TFuture<RawResponse> future1 = scenario.sendRequest(100);
            TFuture<RawResponse> future2 = scenario.sendRequest(100);

            scenario.releaseResponses(2);

            assertEquals(ResponseCode.OK, future1.get().getResponseCode());
            assertEquals(ResponseCode.OK, future2.get().getResponseCode());
        }
    }

    private static final class Scenario implements AutoCloseable {

        private Semaphore responseGate;
        private InetAddress host;
        private TChannel server;
        private SubChannel subServer;
        private TChannel client;
        private SubChannel subClient;

        Scenario(int low, int high) throws Exception {
            responseGate = new Semaphore(0);

            host = InetAddress.getByName(null);

            server = new TChannel.Builder("server")
                .setServerHost(host)
                .setChildLoadControl(low, high)
                .build();

            subServer = server.makeSubChannel("server")
                .register("echo", new GatedResponseHandler(responseGate));

            client = new TChannel.Builder("client")
                .setServerHost(host)
                .build();

            subClient = client.makeSubChannel("server");

            server.listen();
            client.listen();
        }

        void releaseResponses(int amount) {
            responseGate.release(amount);
        }

        TFuture<RawResponse> sendRequest(int timeout) {
            return subClient.send(createRequest(timeout), host, server.getListeningPort());
        }

        RawRequest createRequest(int timeout) {
            return new RawRequest.Builder("server", "echo")
                .setTimeout(timeout)
                .build();
        }

        @Override
        public void close() throws Exception {
            server.shutdown();
            client.shutdown();
        }
    }

    private static final class GatedResponseHandler implements RequestHandler {

        private final Semaphore responseGate;

        public GatedResponseHandler(Semaphore responseGate) {
            this.responseGate = responseGate;
        }

        @Override
        public Response handle(Request request) {
            try {
                responseGate.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return new RawResponse.Builder(request).build();
        }
    }
}
