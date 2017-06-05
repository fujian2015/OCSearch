package com.asiainfo.ocsearch.datainput.batch.map;

/**
 * Created by Aaron on 17/5/26.
 */
import com.asiainfo.ocsearch.datainput.util.ColumnField;
import com.asiainfo.ocsearch.datainput.util.ColumnMapConverter;
import com.asiainfo.ocsearch.expression.Engine;
import com.asiainfo.ocsearch.expression.Executor;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BulkloadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    private String hbaseTable;
    private String dataSeparator;
//    private String columnMapStr;
//    private String fieldSequenceStr;
//    private String rowKeyExpression;
//    private ColumnMapConverter columnMapConverter = ColumnMapConverter.getInstance();
    private Engine expressionEngine = Engine.getInstance();
    private Executor executor;
    private Map<String,ColumnField> columnFamilyMap;
    private Map<String,Integer> fieldSequenceMap;
//    private Map<String,Object> dataMap;


    public void setup(Context context) {
        Gson gson = new Gson();
        Configuration configuration = context.getConfiguration();//获取作业参数
        hbaseTable = configuration.get("hbase.table.name");
        dataSeparator = configuration.get("data.separator");
        String rowKeyExpression = configuration.get("hbase.table.rowkey.expression");
        String columnMapStr = configuration.get("hbase.column.family.map");
        String fieldSequenceStr = configuration.get("hbase.field.sequence");
        columnFamilyMap = gson.fromJson(columnMapStr,new TypeToken<Map<String,ColumnField>>(){}.getType());
        fieldSequenceMap = gson.fromJson(fieldSequenceStr,new TypeToken<Map<String,Integer>>(){}.getType());
        executor = expressionEngine.createExecutor(rowKeyExpression);
    }
    public void map(LongWritable key, Text value, Context context){
        try {
            String[] values = value.toString().split(dataSeparator);
            Map<String,Object> dataMap = dataInitial(values);
            byte[] rowKeyBytes = Bytes.toBytes(executor.evaluate(dataMap).toString());
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowKeyBytes);
            Put put = dataPutfromValues(rowKeyBytes,values);
            context.write(rowKey, put);
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }

    private Map<String,Object> dataInitial(String[] values) {

        Map<String,Object> resultMap = new HashMap<>();

        for(Map.Entry<String,Integer> entry : fieldSequenceMap.entrySet()) {
            resultMap.put(entry.getKey(),values[entry.getValue()-1]);
        }

        return resultMap;
    }

    private Put dataPutfromValues(byte[] rowkey,String[] values) {
        Put put = new Put(rowkey);
        String columnFamily;
        String column;
        String content = "";
        for (Map.Entry<String,ColumnField> entry : columnFamilyMap.entrySet()) {

            columnFamily = entry.getKey().split(":")[0];
            column = entry.getValue().getColumn();

            if(entry.getValue().getSeperator()!=null) {

                Iterator<Integer> iterator = entry.getValue().getSequence().iterator();
                while(iterator.hasNext())
                {
                    content = content.concat(values[iterator.next()-1]);
                    if(iterator.hasNext())
                        content = content.concat(entry.getValue().getSeperator());
                }

            }else {
                content = values[entry.getValue().getSequence().get(0)];
            }

            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(content));
        }
        return put;
    }


}
