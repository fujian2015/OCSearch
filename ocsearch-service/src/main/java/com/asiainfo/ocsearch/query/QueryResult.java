package com.asiainfo.ocsearch.query;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;

/**
 * Created by mac on 2017/5/16.
 */
public class QueryResult {

    int total = 0;
   ArrayNode data = JsonNodeFactory.instance.arrayNode();

    Exception lastError=null;

    public void addData(JsonNode jsonNode) {
        data.add(jsonNode);
        total++;
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getTotal() {
        return total;
    }

    public ArrayNode getData() {
        return data;
    }

    public void setData(ArrayNode data) {
        this.data = data;
    }

    public Exception getLastError() {
        return lastError;
    }

    public void setLastError(Exception lastError) {
        this.lastError = lastError;
    }
}
