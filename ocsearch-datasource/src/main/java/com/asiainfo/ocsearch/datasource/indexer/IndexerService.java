package com.asiainfo.ocsearch.datasource.indexer;

import org.apache.log4j.Logger;

import java.io.File;

/**
 * Created by mac on 2017/4/5.
 */
public class IndexerService {

    final String indexerHome;
    final String zkHost;
    final String solrZk;
    Logger logger = Logger.getLogger(this.getClass());


    public IndexerService(String indexerHome, String zkHost, String solrZk) {

        this.indexerHome = indexerHome;
        this.zkHost = zkHost;
        this.solrZk = solrZk;
    }

    /**
     * ./hbase-indexer add-indexer
     * -n myIndexer1
     * -z 10.1.245.118:2181:2181
     * -c morphline-hbase-mapper.xml
     * -cp solr.zk=10.1.245.118:2181/
     * -cp solr.collection=mycoll
     *
     * @param table
     * @param confXml
     * @param  solrCollection
     */
    public void createTable(String table, String confXml, String solrCollection)  {

        StringBuilder cmd = new StringBuilder("./bin/hbase-indexer add-indexer");

        cmd.append(" -n " + table);
        cmd.append(" -z " + zkHost);
        cmd.append(" -c " + confXml);
        cmd.append(" -cp solr.zk=" + solrZk);
        cmd.append(" -cp solr.collection=" + solrCollection);

        try {
            ExecuteProcessUtil.execute(cmd.toString(), new File(indexerHome), logger);
        }catch (Exception e){
            new RuntimeException("create hbase indexer table failure!",e);
        }
    }
    /**
     * ./hbase-indexer delete-indexer
     * -n myIndexer1
     * -z 10.1.245.118:2181:2181
     * @param table
     *
     */
    public void deleteTable(String table) {
        StringBuilder cmd = new StringBuilder("./bin/hbase-indexer delete-indexer");
        cmd.append(" -n " + table);
        cmd.append(" -z " + zkHost);

        try {
            ExecuteProcessUtil.execute(cmd.toString(), new File(indexerHome), logger);
        }catch (Exception e){
            new RuntimeException("delete hbase indexer table failure!",e);
        }
    }
    public boolean exists(String table){

        StringBuilder cmd = new StringBuilder("./bin/hbase-indexer list-indexers");

        cmd.append(" -z " + zkHost);

        try {
            String response=ExecuteProcessUtil.execute(cmd.toString(), new File(indexerHome), logger);

            String res[]=response.substring(response.indexOf("Number of indexes:")).split("\n");
            for(String re:res){
                if(re.startsWith(table))
                    return true;
            }
        }catch (Exception e){
            new RuntimeException("check hbase indexer table failure!",e);
        }
        return false;
    }

}
