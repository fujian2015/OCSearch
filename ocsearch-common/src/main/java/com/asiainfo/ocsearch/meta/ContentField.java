package com.asiainfo.ocsearch.meta;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.Serializable;

/**
 * Created by mac on 2017/4/17.
 */
public class ContentField implements Serializable, Cloneable {
    String name;
    String type;

    public ContentField(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public ContentField(JsonNode content) {
        this(content.get("name").asText(), content.get("type").asText());
    }

    public JsonNode toJsonNode() {

        ObjectNode contentNode = JsonNodeFactory.instance.objectNode();
        contentNode.put("name", name);
        contentNode.put("type", type);
        return contentNode;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }


    @Override
    public String toString() {
        return toJsonNode().toString();
    }

    @Override
    public Object clone() {
        return new ContentField(this.name, this.type);
    }

}
