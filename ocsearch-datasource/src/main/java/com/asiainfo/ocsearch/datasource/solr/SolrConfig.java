package com.asiainfo.ocsearch.datasource.solr;

import java.util.Properties;

/**
 * Created by mac on 2017/3/22.
 */
public class SolrConfig {

    private final String zk;

    private final int soTimeout;
    private final int zkClientTimeout;
    private final int zkConnectTimeout;
    private final int replicas;

    public int getMaxShardsPerNode() {
        return maxShardsPerNode;
    }

    public boolean isAutoAddReplicas() {
        return autoAddReplicas;
    }

    private final int maxShardsPerNode;
    private final boolean autoAddReplicas;


    public SolrConfig(Properties prop) {

        zk=prop.getProperty("solr.zookeeper",null);
        soTimeout=Integer.parseInt(prop.getProperty("solr.soTimeout","60000"));
        zkClientTimeout=Integer.parseInt(prop.getProperty("solr.zkClientTimeout","60000"));
        zkConnectTimeout=Integer.parseInt(prop.getProperty("solr.zkConnectTimeout","60000"));
        replicas = Integer.parseInt(prop.getProperty("solr.replicas","1"));
        maxShardsPerNode = Integer.parseInt(prop.getProperty("solr.maxShardsPerNode","2"));
        autoAddReplicas =Boolean.parseBoolean(prop.getProperty("solr.autoAddReplicas","true"));

    }

    public String getZookeeper(){
        return zk;
    }

    public int getZkClientTimeout(){
        return zkClientTimeout;
    }
    public int getZkConnectTimeout(){
        return zkConnectTimeout;
    }
    public int getSoTimeout(){
        return soTimeout;
    }

    public int getReplicas() {
        return replicas;
    }

}
