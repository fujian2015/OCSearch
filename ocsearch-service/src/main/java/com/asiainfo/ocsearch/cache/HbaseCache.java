package com.asiainfo.ocsearch.cache;

import com.asiainfo.ocsearch.datasource.hbase.AdminCallBack;
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

    final String family = "A";
    final String column = "1";

    String table;
    GetService hbaseService = HbaseServiceManager.getInstance().getGetService();

    public HbaseCache(String table, int ttl) {
        AdminService adminService = HbaseServiceManager.getInstance().getAdminService();
        if (!adminService.existsTable(table)) {

            boolean exist = adminService.execute(new AdminCallBack<Boolean>() {
                @Override
                public Boolean doWithAdmin(Admin admin) throws Exception {
                    HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(table));

                    tableDesc.setDurability(Durability.SKIP_WAL);
                    HColumnDescriptor cd = new HColumnDescriptor(family);
                    cd.setBlockCacheEnabled(true);
                    cd.setTimeToLive(ttl);

                    tableDesc.addFamily(cd);

                    admin.createTable(tableDesc);
                    return true;
                }
            });
        }
        this.table = table;
    }

    @Override
    public void put(String key, String o) throws Exception {

        Put put = new Put(key.getBytes());
        put.addColumn(family.getBytes(), column.getBytes(), o.getBytes());
        boolean succcess = hbaseService.execute(table, htable -> {
            htable.put(put);
            return true;
        });
    }

    @Override
    public String get(String key) throws Exception {

        return Bytes.toString(hbaseService.get(table, key.getBytes(), family.getBytes(), column.getBytes()));
    }

    @Override
    public void remove(String key) throws Exception {
        Delete delete = new Delete(key.getBytes());
        boolean sucess = hbaseService.execute(table, htable -> {
            htable.delete(delete);
            return true;
        });
    }

    @Override
    public void put(String primaryKey, Map<String, String> kv) throws Exception {
        Put put = new Put(primaryKey.getBytes());
        kv.keySet().forEach(k -> put.addColumn(family.getBytes(), k.getBytes(), kv.get(k).getBytes()));
        boolean sucess = hbaseService.execute(table, htable -> {
            htable.put(put);
            return true;
        });
    }

    @Override
    public Map<String, String> get(String primaryKey, List<String> secondaryKeys) throws Exception {
        Get get = new Get(primaryKey.getBytes());
        secondaryKeys.forEach(column -> get.addColumn(family.getBytes(), column.getBytes()));
        Result result = hbaseService.getResult(table, get);
        Map<String, String> resultMap = new HashMap();
        if (result != null && false == result.isEmpty())
            result.getFamilyMap(family.getBytes()).entrySet().forEach(entry -> {
                resultMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            });
        return resultMap;
    }

}
