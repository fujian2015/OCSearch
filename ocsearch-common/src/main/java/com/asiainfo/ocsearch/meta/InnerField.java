package com.asiainfo.ocsearch.meta;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.Serializable;

/**
 * Created by mac on 2017/5/14.
 */
public class InnerField implements Serializable, Cloneable {
    String name;
    String separator;
    String hbaseColumn;
    String hbaseFamily;

    public InnerField(JsonNode jsonNode) {

        this.name = jsonNode.get("name").asText();

        this.separator = jsonNode.get("separator").asText();
        if (jsonNode.has("hbase_column")) {
            this.hbaseColumn = jsonNode.get("hbase_column").asText();
            this.hbaseFamily = jsonNode.get("hbase_family").asText();
        }
    }

    public InnerField(String name, String separator, String hbaseColumn, String hbaseFamily) {
        this.name = name;
        this.separator = separator;
        this.hbaseColumn = hbaseColumn;
        this.hbaseFamily = hbaseFamily;
    }

    public JsonNode toJsonNode() {
        ObjectNode innerNode = new ObjectMapper().createObjectNode();

        innerNode.put("name", name);
        innerNode.put("separator", separator);

        if (hbaseColumn != null) {
            innerNode.put("hbase_column", hbaseColumn);
            innerNode.put("hbase_family", hbaseFamily);
        }
        return innerNode;
    }

    @Override
    public String toString() {
        return toJsonNode().toString();
    }


    @Override
    public Object clone() {
        return new InnerField(this.name, this.separator, this.hbaseColumn, this.hbaseFamily);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        this.separator = separator;
    }

    public String getHbaseColumn() {
        return hbaseColumn;
    }

    public void setHbaseColumn(String hbaseColumn) {
        this.hbaseColumn = hbaseColumn;
    }

    public String getHbaseFamily() {
        return hbaseFamily;
    }

    public void setHbaseFamily(String hbaseFamily) {
        this.hbaseFamily = hbaseFamily;
    }
}
