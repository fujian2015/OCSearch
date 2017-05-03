package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mac on 2017/4/19.
 */
public class GetService extends AbstractService {

    Logger log = Logger.getLogger(getClass());


    //多个HTable用同一个connection,同一个pool

    public GetService(Connection connection) {
        super(connection);
    }

    public Result getResult(String tableName, final Get get) {

        Result result;
        try {
            result = getTable(tableName).get(get);

        } catch (IOException e) {
            throw new RuntimeException("Exception occured in getResult method, tableName=" + tableName, e);
        }
        return result;

    }

    public byte[] get(String tableName, byte[] row, final byte[] family, final byte[] qualifier) {
        final Get get = new Get(row);
        get.addColumn(family, qualifier);

        Result result = this.getResult(tableName, get);
        if (result.isEmpty())
            return null;
        return result.getValue(family, qualifier);
    }

    public Map<byte[], byte[]> gets(String tableName, byte[] row, final byte[] family, final byte[]... qualifiers) {

        Map<byte[], byte[]> resultMap = new HashMap<>();
        final Get get = new Get(row);
        for (byte[] qualifier : qualifiers) {
            get.addColumn(family, qualifier);
        }

        Result result = this.getResult(tableName, get);
        if (!result.isEmpty()) {
            for (byte[] qualifier : qualifiers) {

                resultMap.put(qualifier,
                        result.getValue(family, qualifier));
            }
        }

        return resultMap;
    }


    public boolean exists(String tableName, final byte[] rowkey) {

        final Get get = new Get(rowkey);

        return this.exists(tableName, get);

    }

    public boolean exists(String tableName, final Get get) {
        try {
            return getTable(tableName).exists(get);
        } catch (IOException e) {
            throw new RuntimeException("Exception occured in exists method, tableName=" + tableName, e);
        }
    }

    public boolean exists(String tableName, byte[] row, byte[] family) {
        Get get = new Get(row);
        get.addFamily(family);
        return exists(tableName, get);
    }

    public boolean exists(String tableName, byte[] row, byte[] family,
                          byte[] qualifier) {
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        return exists(tableName, get);
    }


}
