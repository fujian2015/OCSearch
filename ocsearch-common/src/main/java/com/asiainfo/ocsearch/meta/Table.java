package com.asiainfo.ocsearch.meta;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

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
    TreeSet<String> regionSplits;

    public Table(JsonNode request) throws ServiceException {
        try {
            String name = request.get("name").asText();

            String schema = request.get("schema").asText();

            JsonNode hbaseNode = request.get("hbase");

            int regions = -1;

            if (hbaseNode.get("region_num") != null)
                regions = hbaseNode.get("region_num").getIntValue();

            TreeSet<String> regionsSplits = new TreeSet<>();

            if (hbaseNode.get("region_split") != null) {
                ArrayNode regionList = (ArrayNode) hbaseNode.get("region_split");
                regionList.forEach(jsonNode -> regionsSplits.add(jsonNode.asText()));
            }

            JsonNode solrNode = request.get("solr");

            int solrShards = solrNode.get("shards").asInt();

            int solrReplicas = -1;

            if (solrNode.get("replicas") != null) {
                solrReplicas = solrNode.get("replicas").asInt();
            }

            init(name, schema, solrShards, solrReplicas, regions, regionsSplits);

        } catch (Exception e) {
            throw new ServiceException("parse error!", ErrorCode.PARSE_ERROR);
        }
    }

    public Table(String name, String schema, int solrShards, int solrReplicas, int hbaseRegions, TreeSet<String> regionSplits) {
        init(name, schema, solrShards, solrReplicas, hbaseRegions, regionSplits);
    }

    public void init(String name, String schema, int solrShards, int solrReplicas, int hbaseRegions, TreeSet<String> regionSplits) {
        this.name = name;
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

    public JsonNode toJsonNode() {

        JsonNodeFactory jf = JsonNodeFactory.instance;

        ObjectNode hbaseNode = jf.objectNode();

        hbaseNode.put("region_num", this.hbaseRegions);
        ArrayNode regionNode = JsonNodeFactory.instance.arrayNode();
        this.regionSplits.forEach(regionSplit -> regionNode.add(regionSplit));
        hbaseNode.put("region_split", regionNode);

        ObjectNode solrNode = jf.objectNode();

        solrNode.put("shards", solrShards);

        solrNode.put("replicas", solrReplicas);

        ObjectNode table = jf.objectNode();
        table.put("name", this.name);
        table.put("schema", this.schema);
        table.put("hbase", hbaseNode);
        table.put("solr", solrNode);

        return table;
    }
    @Override
    public String toString(){
        return toJsonNode().toString();
    }


}
