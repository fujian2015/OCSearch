package com.asiainfo.ocsearch.db.solr;

import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;

import java.util.Properties;

/**
 * Created by mac on 2017/3/22.
 */
public class SolrConfig {

    private final String zk;

    private final int soTimeout;
    private final int zkClientTimeout;
    private final int zkConnectTimeout;



    public SolrConfig(String configFile) {

        Properties prop=PropertiesLoadUtil.loadProFile(configFile);

        zk=prop.getProperty("zookeeper",null);
        soTimeout=Integer.parseInt(prop.getProperty("soTimeout","60000"));
        zkClientTimeout=Integer.parseInt(prop.getProperty("zkClientTimeout","60000"));
        zkConnectTimeout=Integer.parseInt(prop.getProperty("zkConnectTimeout","60000"));

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

}
