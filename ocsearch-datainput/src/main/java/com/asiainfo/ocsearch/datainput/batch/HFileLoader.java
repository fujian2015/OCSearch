package com.asiainfo.ocsearch.datainput.batch;

/**
 * Created by Aaron on 17/5/26.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HFileLoader {
    public static void doBulkLoad(String pathToHFile, String tableName){
        try {
            Configuration configuration = new Configuration();
            HBaseConfiguration.addHbaseResources(configuration);
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);
            HTable hTable = new HTable(configuration, tableName);//指定表名
            loadFfiles.doBulkLoad(new Path(pathToHFile), hTable);//导入数据
            System.out.println("Bulk Load Completed..");
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

}