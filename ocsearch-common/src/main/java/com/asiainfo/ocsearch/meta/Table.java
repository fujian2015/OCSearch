package com.asiainfo.ocsearch.meta;

import java.io.Serializable;
import java.util.TreeSet;

/**
 * Created by mac on 2017/4/17.
 */
public class Table implements Serializable {
    String name;
    String schema;

    int hbaseRegions = -1;
    int solrShards = -1;
    int solrReplicas = -1;
    TreeSet<String> regionSplits ;

    public Table(String name,String schema, int solrShards, int solrReplicas, int hbaseRegions, TreeSet<String> regionSplits) {

        this.name=name;
        this.schema = schema;
        this.solrShards = solrShards;
        this.solrReplicas = solrReplicas;
        this.hbaseRegions = hbaseRegions;
        this.regionSplits = regionSplits;
    }

    public String getSchema() {
        return schema;
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




}
