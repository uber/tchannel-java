package com.uber.tchannel.handlers;

import static com.uber.tchannel.handlers.LoadControlHandler.Factory.MAX_HIGH;
import static org.junit.Assert.assertEquals;

import com.uber.tchannel.api.ResponseCode;
import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TChannel;
import com.uber.tchannel.api.TChannel.Builder;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.RequestHandler;
import com.uber.tchannel.channels.Connection;
import com.uber.tchannel.channels.ConnectionState;
import com.uber.tchannel.channels.Peer;
import com.uber.tchannel.errors.ErrorType;
import com.uber.tchannel.messages.RawRequest;
import com.uber.tchannel.messages.RawResponse;
import com.uber.tchannel.messages.Request;
import com.uber.tchannel.messages.Response;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
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

    @Test(expected = IllegalArgumentException.class)
    public void invariant_highTooBig() {
        new TChannel.Builder("server")
            .setChildLoadControl(0, MAX_HIGH + 1)
            .build();
    }

    @Test
    public void testHighWaterMark() throws Exception {

        try (Scenario scenario = new Scenario(0, 2)) {
            ArrayList<TFuture<RawResponse>> futures = new ArrayList<>();
            futures.add(scenario.sendRequest(1000));
            Thread.sleep(100); // required to avoid write batching
            futures.add(scenario.sendRequest(1000));
            Thread.sleep(100); // required to avoid write batching
            futures.add(scenario.sendRequest(1000));
            Thread.sleep(100); // required to avoid write batching

            // Only two requests reached the handler (third request not read)
            assertEquals(2, scenario.responseGate.getQueueLength());

            // Allow handler to respond
            scenario.releaseResponses(futures.size());

            // All requests should be resolved
            for (TFuture<RawResponse> future : futures) {
                assertEquals(ResponseCode.OK, future.get().getResponseCode());
            }
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
