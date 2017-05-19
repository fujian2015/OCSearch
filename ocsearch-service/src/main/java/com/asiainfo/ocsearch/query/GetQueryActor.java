package com.asiainfo.ocsearch.query;

import com.asiainfo.ocsearch.datasource.hbase.GetService;
import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by mac on 2017/5/16.
 */
public class GetQueryActor extends QueryActor {

    static Logger logger = Logger.getLogger(GetQueryActor.class);

    List<String> rowKeys;

    public GetQueryActor(HbaseQuery hbaseQuery, List<String> rowKeys,CountDownLatch runningThreadNum) {
        super(hbaseQuery,runningThreadNum);
        this.rowKeys = rowKeys;
    }

    @Override
    public void run() {
        try {
            GetService getService = HbaseServiceManager.getInstance().getGetService();

            List<Pair<byte[], byte[]>> columnFamiles = this.hbaseQuery.getColumns();

            if (rowKeys.size() == 1) {
                Get get = new Get(Bytes.toBytes(rowKeys.get(0)));
                get.setCacheBlocks(true);
                columnFamiles.forEach(column -> get.addColumn(column.getFirst(), column.getSecond()));
                Result result = getService.getResult(hbaseQuery.table, get);
                this.queryResult.addData(hbaseQuery.extractResult(result));

            } else {
                List<Get> gets = new ArrayList<>(rowKeys.size());
                rowKeys.forEach(rowkey -> {
                    Get get = new Get(Bytes.toBytes(rowkey));
                    gets.add(get);
                    get.setCacheBlocks(true);
                    columnFamiles.forEach(column -> get.addColumn(column.getFirst(), column.getSecond()));
                });

                Result[] results = getService.getList(hbaseQuery.table, gets);
                for (Result result : results) {
                    if (result != null) {
                        this.queryResult.addData(hbaseQuery.extractResult(result));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("occur error in get query,exceptions:", e);
            this.queryResult.setLastError(e);
        }

    }

}
