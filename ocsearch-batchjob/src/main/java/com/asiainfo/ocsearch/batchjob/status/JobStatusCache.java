package com.asiainfo.ocsearch.batchjob.status;

import com.asiainfo.ocsearch.cache.ICache;
import com.asiainfo.ocsearch.datasource.hbase.AdminService;
import com.asiainfo.ocsearch.datasource.hbase.GetService;
import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobStatusCache implements ICache{

    public static final String startTimeColumn = "STARTTIME";
    public static final String endTimeColumn = "ENDTIME";
    public static final String statusColumn = "STATUS";

    static final String family = "A";
    String table;
    GetService hbaseService = HbaseServiceManager.getInstance().getGetService();

    public JobStatusCache(String table, int ttl) {
        AdminService adminService = HbaseServiceManager.getInstance().getAdminService();
        if (!adminService.existsTable(table)) {

            adminService.execute(admin -> {
                HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(table));

                tableDesc.setDurability(Durability.SKIP_WAL);
                HColumnDescriptor cd = new HColumnDescriptor(family);
                cd.setBlockCacheEnabled(true);
                cd.setTimeToLive(ttl);

                tableDesc.addFamily(cd);

                admin.createTable(tableDesc);
                return true;
            });
        }
        this.table = table;
    }

    @Override
    public void put(String key, String o) throws Exception {
    }

    @Override
    public String get(String key) throws Exception {

        String column = "1";
        return Bytes.toString(hbaseService.get(table, Bytes.toBytes(key), Bytes.toBytes(family), Bytes.toBytes(column)));
    }

    public Map<byte[],byte[]> get(String rowkey,ArrayList<String> columnsArr) {
//        this.columnsArr = columnsArr;
        Map<byte[],byte[]> result;
        int size = columnsArr.size();
        byte[][] columns = new byte[size][];
        for(int i=0;i<size;i++){
            columns[i]=columnsArr.get(i).getBytes();
        }
        result = hbaseService.gets(table,Bytes.toBytes(rowkey), Bytes.toBytes(family), columns);
        return result;
    }


    @Override
    public void remove(String key) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(key));
        hbaseService.execute(table, htable -> {
            htable.delete(delete);
            return true;
        });
    }

    @Override
    public void put(String primaryKey, Map<String, String> kv) throws Exception {
        Put put = new Put(Bytes.toBytes(primaryKey));
        kv.keySet().forEach(k -> put.addColumn(family.getBytes(), k.getBytes(), kv.get(k).getBytes()));
        hbaseService.execute(table, htable -> {
            htable.put(put);
            return true;
        });
    }

    @Override
    public Map<String, String> get(String primaryKey, List<String> columnsArr) throws Exception {
        Map<String,String> result = new HashMap<>();
        int size = columnsArr.size();
        byte[][] columns = new byte[size][];
        for(int i=0;i<size;i++){
            columns[i]=columnsArr.get(i).getBytes();
        }
        Map<byte[],byte[]> bytesMap = hbaseService.gets(table,Bytes.toBytes(primaryKey), Bytes.toBytes(family), columns);
        for(Map.Entry<byte[],byte[]> entry: bytesMap.entrySet()) {

            byte[] key = entry.getKey();
            byte[] value = entry.getValue();

            result.put(new String(key), new String(value));
        }
        return result;
    }

}
