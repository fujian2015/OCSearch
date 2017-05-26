package com.asiainfo.ocsearch.service.query;

/**
 * Created by mac on 2017/5/23.
 */
public class OCRowKey {

    String rowKey;
    String table;

    public OCRowKey(String table, String rowKey) {
        this.rowKey = rowKey;
        this.table = table;
    }
}
