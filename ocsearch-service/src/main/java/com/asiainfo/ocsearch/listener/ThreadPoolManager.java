package com.asiainfo.ocsearch.listener;

import com.asiainfo.ocsearch.constants.OCSearchEnv;
import org.apache.hadoop.hbase.util.Threads;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by mac on 2017/5/18.
 */
public class ThreadPoolManager {

    static  Map<String, ExecutorService> treadPools = new ConcurrentHashMap();

    public static ExecutorService getExecutor(String pool) {
        return treadPools.get(pool);
    }
    public static void setUp(){

        initGetPool();
        initScanPool();

        boolean hbaseOnly = Boolean.valueOf(OCSearchEnv.getEnvValue("HBASE_ONLY"));
        if (!hbaseOnly) {
           initSolrPool();
        }

    }
    public static  void initSolrPool(){
        int coreSize=Integer.parseInt(OCSearchEnv.getEnvValue("solr.threadpool.core.size"));
        int maxSize=Integer.parseInt(OCSearchEnv.getEnvValue("solr.threadpool.max.size"));

        treadPools.put("solrQuery",new ThreadPoolExecutor(coreSize,maxSize,60l, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("solrQuery")));
    }
    public static  void initGetPool(){
        int coreSize=Integer.parseInt(OCSearchEnv.getEnvValue("get.threadpool.core.size"));
        int maxSize=Integer.parseInt(OCSearchEnv.getEnvValue("get.threadpool.max.size"));

        treadPools.put("getQuery",new ThreadPoolExecutor(coreSize,maxSize,60l, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("getQuery")));
    }
    public static  void initScanPool(){
        int coreSize=Integer.parseInt(OCSearchEnv.getEnvValue("scan.threadpool.core.size"));
        int maxSize=Integer.parseInt(OCSearchEnv.getEnvValue("scan.threadpool.max.size"));

        treadPools.put("scanQuery",new ThreadPoolExecutor(coreSize,maxSize,60l, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(), Threads.newDaemonThreadFactory("scanQuery")));
    }

    public  static void shutdownAll(){
        treadPools.values().forEach(executorService -> executorService.shutdownNow());
        treadPools.values().forEach(executorService -> {
            try {
                executorService.awaitTermination(5, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
