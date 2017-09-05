package com.asiainfo.ocsearch.query;

import com.asiainfo.ocsearch.datasource.hbase.GetService;
import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.codehaus.jackson.node.JsonNodeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by mac on 2017/5/16.
 */
public class GetQueryActor extends QueryActor {

    static Logger logger = Logger.getLogger(GetQueryActor.class);


    public GetQueryActor(HbaseQuery hbaseQuery,CountDownLatch runningThreadNum) {
        super(hbaseQuery,runningThreadNum);

    }

    @Override
    public void run() {
        try {
            GetService getService = HbaseServiceManager.getInstance().getGetService();

            List<Pair<byte[], byte[]>> columnFamilies = this.hbaseQuery.getColumns();
            UniqueKeyFormatter uniqueKeyFormatter = hbaseQuery.getUniqueKeyFormatter();
           List<String> rowKeys= this.hbaseQuery.getRowKeys();
            if (this.hbaseQuery.getRowKeys().size() == 1) {
                Get get = new Get(uniqueKeyFormatter.unformatRow(rowKeys.get(0)));
                get.setCacheBlocks(true);
                columnFamilies.forEach(column -> get.addColumn(column.getFirst(), column.getSecond()));
                Result result = getService.getResult(hbaseQuery.table, get);
                this.queryResult.addData(hbaseQuery.extractResult(result));
            } else {
                List<Get> gets = new ArrayList<>(rowKeys.size());
                rowKeys.forEach(rowkey -> {
                    Get get = new Get(uniqueKeyFormatter.unformatRow(rowkey));
                    gets.add(get);
                    get.setCacheBlocks(true);
                    columnFamilies.forEach(column -> get.addColumn(column.getFirst(), column.getSecond()));
                });

                Result[] results = getService.getList(hbaseQuery.table, gets);
                for (Result result : results) {
                    if (result != null&&!result.isEmpty()) {
                        this.queryResult.addData(hbaseQuery.extractResult(result));
                    }
                    else{
                        this.queryResult.addData(JsonNodeFactory.instance.objectNode());
                    }
                }
            }
            this.queryResult.setTotal(queryResult.getData().size());

        } catch (Exception e) {
            logger.error("occur error in get query,exceptions:", e);
            this.queryResult.setLastError(e);
        }finally {
            runningThreadNum.countDown();
        }

    }

}
