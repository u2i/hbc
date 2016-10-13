package com.twitter.hbc.example;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.ReplayEnterpriseStreamingEndpoint_v2;
import com.twitter.hbc.core.processor.LineStringProcessor;
import com.twitter.hbc.httpclient.auth.BasicAuth;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ReplayEnterpriseStreamV2Example {

    public static void run(String username,
                           String password,
                           String account,
                           String label,
                           String product,
                           String fromDateString,
                           String toDateString) throws InterruptedException, ParseException {
        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

        BasicAuth auth = new BasicAuth(username, password);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
        Date fromDate = formatter.parse(fromDateString);
        Date toDate = formatter.parse(toDateString);

        System.out.println(fromDate);
        System.out.println(toDate);

        ReplayEnterpriseStreamingEndpoint_v2 endpoint = new ReplayEnterpriseStreamingEndpoint_v2(account, product, label, fromDate, toDate);

        // Create a new BasicClient. By default gzip is enabled.
        Client client = new ClientBuilder()
                .name("PowerTrackClient-01")
                .hosts(Constants.REPLAY_ENTERPRISE_STREAM_HOST_v2)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new LineStringProcessor(queue))
                .build();

        // Establish a connection
        client.connect();

        // Do whatever needs to be done with messages
        for (int msgRead = 0; msgRead < 1000; msgRead++) {
            String msg = queue.take();
            System.out.println(msg);
        }

        client.stop();
    }

    public static void main(String[] args)  {
        try {
            ReplayEnterpriseStreamV2Example.run(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
