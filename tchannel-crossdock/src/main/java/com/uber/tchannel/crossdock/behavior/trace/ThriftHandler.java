package com.uber.tchannel.crossdock.behavior.trace;

import com.uber.tchannel.api.SubChannel;
import com.uber.tchannel.api.TFuture;
import com.uber.tchannel.api.handlers.ThriftRequestHandler;
import com.uber.tchannel.crossdock.api.Downstream;
import com.uber.tchannel.crossdock.api.Request;
import com.uber.tchannel.crossdock.api.Response;
import com.uber.tchannel.crossdock.thrift.Data;
import com.uber.tchannel.crossdock.thrift.SimpleService.Call_args;
import com.uber.tchannel.crossdock.thrift.SimpleService.Call_result;
import com.uber.tchannel.messages.JSONSerializer;
import com.uber.tchannel.messages.ThriftRequest;
import com.uber.tchannel.messages.ThriftResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

class ThriftHandler extends ThriftRequestHandler<Call_args, Call_result> {
    static final String ENDPOINT = "SimpleService::Call";

    private static final Logger logger = LoggerFactory.getLogger(ThriftHandler.class);

    private final TraceBehavior behavior;

    ThriftHandler(TraceBehavior behavior) {
        this.behavior = behavior;
    }

    @Override
    public ThriftResponse<Call_result> handleImpl(ThriftRequest<Call_args> thriftRequest) {
        Call_args call = thriftRequest.getBody(Call_args.class);
        logger.info("Received thrift request {}", call);
        Request request = dataToObject(call.getArg(), Request.class);

        Response response = behavior.handleRequest(request);

        return new ThriftResponse.Builder<Call_result>(thriftRequest)
                .setTransportHeaders(thriftRequest.getTransportHeaders())
                .setBody(new Call_result(objectToData(response)))
                .build();
    }

    Response callDownstream(Downstream downstream, InetAddress host, int port) throws Exception {
        ThriftRequest<Call_args> request = new ThriftRequest
                .Builder<Call_args>(downstream.getServiceName(), ENDPOINT)
                .setTimeout(1, TimeUnit.MINUTES)
                .setBody(new Call_args(objectToData(downstream)))
                .build();

        SubChannel subChannel = behavior.tchannel.makeSubChannel(downstream.getServiceName());
        TFuture<ThriftResponse<Call_result>> responsePromise = subChannel.send(
                request,
                host,
                port
        );

        ThriftResponse<Call_result> thriftResponse = responsePromise.get();
        try {
            if (thriftResponse.isError()) {
                throw new Exception("Error response: " + thriftResponse.getError());
            }
            Data data = thriftResponse.getBody(Call_result.class).getSuccess();
            return dataToObject(data, Response.class);
        } finally {
            thriftResponse.release();
        }
    }

    private static <T> T dataToObject(Data data, Class<T> objClass) {
        return jsonToObject(data.getS2(), objClass);
    }

    private static Data objectToData(Object obj) {
        String json = objectToJSON(obj);
        return new Data(false, json, 0);
    }

    private static <T> T jsonToObject(String json, Class<T> objClass) {
        ByteBuf byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(json.length());
        byteBuf.writeBytes(json.getBytes(StandardCharsets.UTF_8));
        T obj = new JSONSerializer().decodeBody(byteBuf, objClass);
        byteBuf.release();
        return obj;
    }

    private static String objectToJSON(Object obj) {
        ByteBuf bytes = new JSONSerializer().encodeBody(obj);
        String json = new String(bytes.array(), StandardCharsets.UTF_8);
        bytes.release();
        return json;
    }
}
