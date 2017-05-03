package com.asiainfo.ocsearch.meta;

import java.io.Serializable;
import java.util.TreeSet;

/**
 * Created by mac on 2017/4/17.
 */
public class Table implements Serializable {
    String name;
    String schema;
    IndexType indexType ;  //-1: hbase ,0 solr+hbase hbase-indexer
    int hbaseRegions = -1;
    int solrShards = -1;
    int solrReplicas = -1;
    TreeSet<String> regionSplits ;

    public Table(String name,String schema, int indexType, int solrShards, int solrReplicas, int hbaseRegions, TreeSet<String> regionSplits) {

        this.name=name;
        this.schema = schema;
        switch (indexType){
            case  -1:
                this.indexType=IndexType.HBASE_ONLY;
                break;
            case 0:
                this.indexType=IndexType.HBASE_SOLR_INDEXER;
                break;
            case 1:
                this.indexType=IndexType.HBASE_SOLR_BATCH;
                break;
            default:
                throw new RuntimeException("unknown index type : "+indexType);
        }

        this.solrShards = solrShards;
        this.solrReplicas = solrReplicas;
        this.hbaseRegions = hbaseRegions;
        this.regionSplits = regionSplits;
    }

    public String getSchema() {
        return schema;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getName() {
        return name;
    }

    public TreeSet<String> getRegionSplits() {
        return regionSplits;
    }

    public int getHbaseRegions() {
        return hbaseRegions;
    }

    public int getSolrReplicas() {
        return solrReplicas;
    }

    public int getSolrShards() {
        return solrShards;
    }

    public enum  IndexType implements Serializable{

        HBASE_ONLY(-1),HBASE_SOLR_INDEXER(0),HBASE_SOLR_BATCH(1);

        int value;
        IndexType(int value) {
            this.value=value;
        }
        public int getValue(){
            return value;
        }

    }


}
