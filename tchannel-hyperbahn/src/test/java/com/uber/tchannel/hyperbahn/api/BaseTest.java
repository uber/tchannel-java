package com.uber.tchannel.hyperbahn.api;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.Properties;

@Ignore
public class BaseTest {
    @BeforeClass
    public static void setUpBaseClass() {
        setupLogger();
    }

    // Configure file format
    //    # Set root logger level to WARN and its only appender to A1.
    //    log4j.rootLogger=WARN, A1
    //
    //    # A1 is set to be a ConsoleAppender.
    //        log4j.appender.A1=org.apache.log4j.ConsoleAppender
    //
    //    # A1 uses PatternLayout.
    //        log4j.appender.A1.layout=org.apache.log4j.PatternLayout
    //    log4j.appender.A1.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n

    public static void setupLogger() {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());

        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "WARN, A1");
        properties.setProperty("log4j.appender.A1", "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.A1.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.A1.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");
        PropertyConfigurator.configure(properties);
    }
}
