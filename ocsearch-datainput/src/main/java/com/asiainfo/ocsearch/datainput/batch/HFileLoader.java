package com.asiainfo.ocsearch.datainput.batch;

/**
 * Created by Aaron on 17/5/26.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HFileLoader {
    public static void doBulkLoad(String pathToHFile, String table){
        try {
            Configuration configuration = new Configuration();

            HBaseConfiguration.addHbaseResources(configuration);

            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles(configuration);

            TableName tableName = TableName.valueOf(table);

            Connection connection = ConnectionFactory.createConnection(configuration);

            Table hbasetable = connection.getTable(tableName);

            Admin admin = connection.getAdmin();

            RegionLocator regionLocator= connection.getRegionLocator(tableName);

            loadFfiles.doBulkLoad(new Path(pathToHFile),admin,hbasetable,regionLocator);

            System.out.println("Bulk Load Completed..");
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

}