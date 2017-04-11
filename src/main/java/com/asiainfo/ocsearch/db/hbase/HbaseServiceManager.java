package com.asiainfo.ocsearch.db.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Threads;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by mac on 2017/4/1.
 */
public class HbaseServiceManager {

    private static HBaseService instance = null;

    public static HBaseService gethBaseService() {
        if (instance == null) {
            try {
                instance = HbaseServiceManager.newInstance("hbase-site.xml");
            } catch (Exception e) {
                instance = null;
                throw new RuntimeException("create hbase service failure!", e);
            }
        }
        return instance;
    }

    private static HBaseService newInstance(Configuration conf) throws Exception {
        Connection conn = null;
        ThreadPoolExecutor pool = null;

        try {
            conn = ConnectionFactory.createConnection(conf,  getDefaultExecutor(conf));
            return new HBaseService(conn);
        } catch (Exception e) {
            if (pool != null) {
                try {
                    pool.shutdown();
                    pool.awaitTermination(60, TimeUnit.SECONDS);
                } catch (InterruptedException e1) {

                }
            }
            throw e;
        }
    }

    private static HBaseService newInstance(String confxml) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(confxml);
        conf = HBaseConfiguration.create(conf);
        return newInstance(conf);
    }

    public static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
        int maxThreads = conf.getInt("hbase.htable.threads.max", 10240);
        if (maxThreads == 0) {
            maxThreads = 1; // is there a better default?
        }
        long keepAliveTime = conf.getLong("hbase.htable.threads.keepalivetime", 60);

        ThreadPoolExecutor pool = new ThreadPoolExecutor(Math.min(2, maxThreads), maxThreads, keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("HBaserService"));
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

}
