package com.asiainfo.ocsearch.query;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by mac on 2017/5/31.
 */
public class FileID {

    public String getTable() {
        return table;
    }

    public String getField() {
        return field;
    }

    public String getRowKey() {
        return rowKey;
    }

    String table;
    String field;
    String rowKey;

    public FileID(String table, String field, String rowKey) {
        this.field = field;
        this.table = table;
        this.rowKey = rowKey;
    }

    JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

    public String toString() {

        BASE64Encoder base64Encoder = new BASE64Encoder();
        ObjectNode objectNode = jsonNodeFactory.objectNode();
        objectNode.put("t", table);
        objectNode.put("f", field);
        objectNode.put("r", rowKey);

        String id = "";
        try {
            id = base64Encoder.encode(objectNode.toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return id;
    }

    public static FileID parseId(String oriId) throws IOException {

        BASE64Decoder base64Decoder = new BASE64Decoder();
        JsonNode jsonNode = new ObjectMapper().readTree(base64Decoder.decodeBuffer(oriId));
        String table = jsonNode.get("t").asText();
        String rowKey = jsonNode.get("r").asText();
        String field = jsonNode.get("f").asText();

        if (StringUtils.isEmpty(table) || StringUtils.isEmpty(rowKey) || StringUtils.isEmpty(field))
            throw new IOException();
        return new FileID(table, field, rowKey);
    }

}
