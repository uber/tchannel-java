package com.uber.tchannel.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class TChannelUtilities {
    private static final Logger logger = LoggerFactory.getLogger(TChannelUtilities.class);

    public static final ByteBuf emptyByteBuf = Unpooled.EMPTY_BUFFER;

    public static void setLogLevel(Level level) {
        LogManager.getRootLogger().setLevel(level);
    }

    public static void PrintException(Throwable throwable) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter( writer );
        throwable.printStackTrace( printWriter );
        printWriter.flush();
        System.out.println(throwable.getMessage());
        System.out.println(writer);
    }

    public static InetAddress getCurrentIp() {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface
                .getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) networkInterfaces
                    .nextElement();
                Enumeration<InetAddress> nias = ni.getInetAddresses();
                while(nias.hasMoreElements()) {
                    InetAddress ia= (InetAddress) nias.nextElement();
                    if (!ia.isLinkLocalAddress()
                        && !ia.isLoopbackAddress()
                        && ia instanceof Inet4Address) {
                        return ia;
                    }
                }
            }
        } catch (SocketException e) {
            logger.error("unable to get current IP.", e);
        }
        return null;
    }
}
