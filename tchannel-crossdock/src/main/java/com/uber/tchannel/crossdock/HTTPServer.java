package com.uber.tchannel.crossdock;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netty.handler.codec.http.HttpHeaderNames.*;


public class HTTPServer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(HTTPServer.class);

    private static final int DEFAULT_PORT = 8080;

    private final int port;

    public HTTPServer() {
        this(DEFAULT_PORT);
    }

    public HTTPServer(int port) {
        this.port = port;
    }

    private static class HealthHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        @Override
        protected void channelRead0(
                ChannelHandlerContext channelHandlerContext,
                FullHttpRequest fullHttpRequest
        ) throws Exception {

            logger.warn("Received {}", fullHttpRequest);

            if (HttpUtil.is100ContinueExpected(fullHttpRequest)) {
                channelHandlerContext.writeAndFlush(
                        new DefaultFullHttpResponse(
                                HttpVersion.HTTP_1_1,
                                HttpResponseStatus.CONTINUE));
            }

            HttpResponse response = new DefaultHttpResponse(
                    fullHttpRequest.protocolVersion(),
                    HttpResponseStatus.OK);
            HttpHeaders headers = response.headers();
            headers.set(CONTENT_TYPE, "text/html; charset=UTF-8");

            ByteBuf responseContent = Unpooled.copiedBuffer("OK", CharsetUtil.UTF_8);

            boolean keepAlive = HttpUtil.isKeepAlive(fullHttpRequest);
            if (keepAlive) {
                headers.set(CONTENT_LENGTH, responseContent.readableBytes());
                headers.set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            // write response
            channelHandlerContext.write(response);
            channelHandlerContext.write(responseContent);
            ChannelFuture future = channelHandlerContext.writeAndFlush(
                    LastHttpContent.EMPTY_LAST_CONTENT);

            // Decide whether to close the connection or not.
            if (!keepAlive) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }

    }

    public void run() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast("codec", new HttpServerCodec());
                            ch.pipeline().addLast("aggegator", new HttpObjectAggregator(Integer.MAX_VALUE));
                            ch.pipeline().addLast("harness", new HealthHandler());
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind("0.0.0.0", port).sync();

            f.channel().closeFuture().sync();
        } catch (Exception e) {
            logger.error("Exception in HTTP server", e);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        new HTTPServer().run();
    }

}
