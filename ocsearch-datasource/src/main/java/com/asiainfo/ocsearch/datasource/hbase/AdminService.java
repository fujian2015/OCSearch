package com.asiainfo.ocsearch.datasource.hbase;

import com.asiainfo.ocsearch.datasource.hbase.util.RegionSplitsUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.Set;


public class AdminService extends AbstractService {

    public AdminService(HbaseServiceManager hbaseServiceManager) {
        super(hbaseServiceManager);
    }

    public void createTable(String tableName, Map<String, Integer> columnFamilies, int regions) {

        if (regions == -1) {
            createTable(tableName, columnFamilies, (byte[][]) null);
        } else {
            createTable(tableName, columnFamilies, RegionSplitsUtil.splits(regions - 1));
        }
    }

    public void createTable(String tableName, Map<String, Integer> columnFamilies, Set<String> regionSplits) {

        byte splits[][] = new byte[regionSplits.size()][];
        int i = 0;

        for (String split : regionSplits) {
            splits[i++] = Bytes.toBytes(split);
        }
        createTable(tableName, columnFamilies, splits);
    }

    private boolean createTable(String tableName, Map<String, Integer> columnFamilies, byte[][] regionSplits) {

        return execute(admin -> {

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

//            tableDesc.setDurability(Durability.USE_DEFAULT);
            //maxVersions
            //CompressionType
            //setBloomFilterType
            //setBlockCacheEnabled
//            tableDesc.setConfiguration("REPLICATION_SCOPE","1");
            for (String columnFamily : columnFamilies.keySet()) {

                HColumnDescriptor cd = new HColumnDescriptor(columnFamily);
                cd.setScope(columnFamilies.get(columnFamily));
                tableDesc.addFamily(cd);
            }

            if (regionSplits == null)
                admin.createTable(tableDesc);
            else
                admin.createTable(tableDesc, regionSplits);
            return true;
        });
    }

    public boolean deleteTable(String tableName) {

        return execute(admin -> {
            TableName t = TableName.valueOf(tableName);
            if (!admin.tableExists(t)) {
                throw new RuntimeException(String.format("hbase table :%s does not exist.", tableName));
            } else {
                admin.disableTable(t);
                admin.deleteTable(t);
            }
            return true;
        });
    }

    public boolean existsTable(String tableName) {
        return execute(admin -> admin.tableExists(TableName.valueOf(tableName)));
    }

    public boolean compact(String tableName) {
        return execute(admin -> {
            TableName t = TableName.valueOf(tableName);
            if (!admin.tableExists(t)) {
                throw new RuntimeException(String.format("hbase table :%s does not exist.", tableName));
            } else {
                admin.compact(t);
            }
            return true;
        });
    }

}
