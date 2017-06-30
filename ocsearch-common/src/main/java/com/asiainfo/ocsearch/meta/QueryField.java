package com.asiainfo.ocsearch.meta;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.Serializable;

/**
 * Created by mac on 2017/4/17.
 */
public class QueryField implements Serializable, Cloneable {

    public QueryField(JsonNode jsonNode) {

        this.name = jsonNode.get("name").getTextValue();

        this.weight = jsonNode.get("weight").getIntValue();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    String name;
    int weight;

    public QueryField(String name, int weight) {
        this.name = name;
        this.weight = weight;
    }

    public JsonNode toJsonNode() {
        ObjectNode queryNode = new ObjectMapper().createObjectNode();

        queryNode.put("name", name);
        queryNode.put("weight", weight);

        return queryNode;
    }

    @Override
    public Object clone() {
        return new QueryField(this.name, this.weight);
    }

}