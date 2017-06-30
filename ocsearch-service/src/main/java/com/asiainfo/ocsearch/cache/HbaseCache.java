package com.asiainfo.ocsearch.cache;

import com.asiainfo.ocsearch.datasource.hbase.AdminService;
import com.asiainfo.ocsearch.datasource.hbase.GetService;
import com.asiainfo.ocsearch.datasource.hbase.HbaseServiceManager;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mac on 2017/5/19.
 */
public class HbaseCache implements ICache {

    static final String family = "A";
    static final String column = "1";

    String table;
    GetService hbaseService = HbaseServiceManager.getInstance().getGetService();

    public HbaseCache(String table, int ttl) {
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

        Put put = new Put(Bytes.toBytes(key));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(o));
        hbaseService.execute(table, htable -> {
            htable.put(put);
            return true;
        });
    }

    @Override
    public String get(String key) throws Exception {

        return Bytes.toString(hbaseService.get(table, Bytes.toBytes(key), Bytes.toBytes(family), Bytes.toBytes(column)));
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
    public Map<String, String> get(String primaryKey, List<String> secondaryKeys) throws Exception {
        Get get = new Get(Bytes.toBytes(primaryKey));
        secondaryKeys.forEach(column -> get.addColumn(family.getBytes(), column.getBytes()));
        Result result = hbaseService.getResult(table, get);
        Map<String, String> resultMap = new HashMap();
        if (result != null && false == result.isEmpty())
            result.getFamilyMap(Bytes.toBytes(family)).entrySet().forEach(entry -> {
                resultMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            });
        return resultMap;
    }

}
