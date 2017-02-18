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

    private static void setupLogger() {
        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "DEBUG, A1");
        properties.setProperty("log4j.appender.A1", "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.A1.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.A1.layout.ConversionPattern", "%-4r [%t] %-5p %c %x - %m%n");
        PropertyConfigurator.configure(properties);

        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }
}
