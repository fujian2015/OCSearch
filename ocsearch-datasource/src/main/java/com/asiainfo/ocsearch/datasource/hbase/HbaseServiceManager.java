package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by mac on 2017/4/1.
 */
public class HbaseServiceManager {

    Logger logger = Logger.getLogger(HbaseServiceManager.class);

    private static HbaseServiceManager instance;

    public static void setup(Configuration configuration) throws Exception {
        instance = new HbaseServiceManager(configuration);
    }

    public static void setup(String confXml) throws Exception {
        Configuration conf = new Configuration();
        conf.addResource(confXml);
        conf = HBaseConfiguration.create(conf);
        setup(conf);
    }

    public static HbaseServiceManager getInstance() {
        return instance;
    }

    private Connection connection = null;

    private AdminService adminService = null;
    private GetService getService = null;
    private ScanService scanService = null;


    Configuration config;

    private HbaseServiceManager(Configuration config) throws IOException {
        this.config = config;
        connection = createConnection(config);
    }

    public synchronized AdminService getAdminService() {
        if (adminService == null) {

            adminService = new AdminService(this);
        }
        return adminService;
    }

    public synchronized GetService getGetService() {
        if (getService == null) {
            getService = new GetService(this);
        }
        return getService;
    }

    public synchronized ScanService getScanService() {
        if (scanService == null) {
            scanService = new ScanService(this);
        }
        return scanService;
    }

    public synchronized PutService createPutService(String table) throws IOException {

        return new PutService(this, table);
    }


    private Connection createConnection(Configuration conf) throws IOException {
        return ConnectionFactory.createConnection(conf);
    }

    @Deprecated
    private ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
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

    public void close() {
        try {
            if (connection != null)
                connection.close();
            connection = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public synchronized void reconnect() {
        logger.info("reconnect hbase connection start!");
        try {
            if (!connection.isClosed()) {
                connection.close();
            }
            connection = createConnection(config);
            logger.info("reconnect hbase connection success!");
        } catch (IOException e) {
            logger.error("reconnect hbase connection failure!", e);
        }

    }

    public synchronized Connection getHBaseConnection() throws IOException {

        if (connection.isClosed()) {
            reconnect();
        }
        return connection;
    }
}
