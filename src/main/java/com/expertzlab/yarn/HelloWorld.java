package com.expertzlab.yarn;

import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.internal.Services;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;

/**
 * Created by admin on 19/07/18.
 */
public class HelloWorld {
    public static Logger LOG = LoggerFactory.getLogger(HelloWorld.class);

    public static class HelloWorldRunnable extends AbstractTwillRunnable {
        @Override
        public void run() {
            LOG.info("Hello World. My first distributed application.");
        }
    }

    public static void main(String[] args) throws Exception {
        TwillRunnerService twillRunner =
                new YarnTwillRunnerService(
                        new YarnConfiguration(), "localhost:2181");
        twillRunner.start();

        TwillController controller =
                twillRunner.prepare(new HelloWorldRunnable())
                        .addLogHandler(
                                new PrinterLogHandler(
                                        new PrintWriter(System.out, true)))
                        .start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Futures.getUnchecked(controller.terminate());
                } finally {
                    twillRunner.stop();
                }
            }
        });

        try {
            controller.awaitTerminated();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}