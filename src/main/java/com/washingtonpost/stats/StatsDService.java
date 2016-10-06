package com.washingtonpost.stats;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;

/**
 * Created by findleyr on 5/23/16.
 */
public class StatsDService {
    private static final StatsDClient statsd = new NonBlockingStatsDClient(
            System.getenv("app.name"),                          /* prefix to any stats; may be null or empty string */
            "statsd",                                           /* common case: localhost */
            8125,                                               /* port */
            new String[] {"vpc:"+System.getenv("vpc")}     /* DataDog extension: Constant tags, always applied */
    );

    public static StatsDClient getStatsDClient() { return statsd; }
}
