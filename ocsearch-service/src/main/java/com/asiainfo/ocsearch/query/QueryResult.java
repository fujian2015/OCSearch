package com.asiainfo.ocsearch.query;

import org.codehaus.jackson.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by mac on 2017/5/16.
 */
public class QueryResult {

    int total = 0;
    List<ObjectNode> data = new ArrayList<>();

    Exception lastError = null;

    String nextRowkey;

    public void addData(ObjectNode jsonNode) {
        data.add(jsonNode);
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public int getTotal() {
        return total;
    }

    public List<ObjectNode> getData() {
        return data;
    }

    public void setData(List<ObjectNode> data) {
        this.data = data;
    }

    public Exception getLastError() {
        return lastError;
    }

    public void setLastError(Exception lastError) {
        this.lastError = lastError;
    }

    public String getLastRowkey() {
        return nextRowkey;
    }

    public void setnextRowkey(String lastRowkey) {
        this.nextRowkey = lastRowkey;
    }
}
