package com.asiainfo.ocsearch.datasource.hbase;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mac on 2017/4/19.
 */
public class ScanService extends AbstractService{

    Logger log = Logger.getLogger(getClass());

    public ScanService(HbaseServiceManager hbaseServiceManager) {
        super(hbaseServiceManager);
    }


    public List<Result> queryWithScan(final String tableName, final Scan scan) {

        List<Result> results = new ArrayList<>();
        ResultScanner scanner = null;
        try {
            scanner = getTable(tableName).getScanner(scan);
            for (Result result : scanner) {
                results.add(result);
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception occured in executeWithScan method, tableName=" + tableName + "\n scan=" + scan + "\n", e);

        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return results;

    }

}
