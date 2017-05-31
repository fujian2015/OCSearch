package com.asiainfo.ocsearch.datainput.batch;

/**
 * Created by Aaron on 17/5/26.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BulkloadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private String hbaseTable;
    private String dataSeperator;
    private String columnFamily1;
    private String columnFamily2;
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();//获取作业参数
        hbaseTable = configuration.get("hbase.table.name");
        dataSeperator = configuration.get("data.seperator");
        columnFamily1 = configuration.get("COLUMN_FAMILY_1");
        columnFamily2 = configuration.get("COLUMN_FAMILY_2");
    }
    public void map(LongWritable key, Text value, Context context){
        try {
            String[] values = value.toString().split(dataSeperator);
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(values[0].getBytes());
            Put put = new Put(Bytes.toBytes(values[0]));
            put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("month"), Bytes.toBytes(values[1]));
            put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("day"), Bytes.toBytes(values[2]));
            for (int i = 3; i < values.length; ++i){
                put.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("hour : " + i), Bytes.toBytes(values[i]));
            }
            context.write(rowKey, put);
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

}
