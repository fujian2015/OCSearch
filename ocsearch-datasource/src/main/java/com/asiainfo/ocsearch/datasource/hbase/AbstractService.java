package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by mac on 2017/4/19.
 */
public class AbstractService implements Closeable {

    protected Connection hConnection;

    public AbstractService(Connection conn) {
        this.hConnection = conn;
    }


    protected HTable getTable(String tableName) throws IOException {

        return (HTable) this.hConnection.getTable(TableName.valueOf(tableName));
    }

    @Override
    public void close() throws IOException {

    }
}
