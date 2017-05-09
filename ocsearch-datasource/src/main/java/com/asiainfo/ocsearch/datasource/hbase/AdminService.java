package com.asiainfo.ocsearch.datasource.hbase;

import com.asiainfo.ocsearch.datasource.hbase.util.RegionSplitsUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;


public class AdminService extends AbstractService {


    Logger log = Logger.getLogger(getClass());


    public AdminService(Connection conn) {
        super(conn);
    }


    public void createTable(String tableName, Set<String> columnFamilies, int regions) {


        if (regions == -1) {
            createTable(tableName, columnFamilies, (byte[][]) null);
        } else {
            createTable(tableName, columnFamilies, RegionSplitsUtil.splits(regions-1));
        }

    }

    public void createTable(String tableName, Set<String> columnFamilies, Set<String> regionSplits) {

        byte splits[][] = new byte[regionSplits.size()][];
        int i = 0;

        for (String split : regionSplits) {
            splits[i++] = Bytes.toBytes(split);
        }
        createTable(tableName, columnFamilies, splits);
    }

    private void createTable(String tableName, Set<String> columnFamilies, byte[][] regionSplits) {
        try {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

            tableDesc.setDurability(Durability.SKIP_WAL);
            //maxVersions
            //CompressionType
            //setBloomFilterType
            //setBlockCacheEnabled
            for (String columnFamily : columnFamilies) {

                HColumnDescriptor cd = new HColumnDescriptor(columnFamily);

                tableDesc.addFamily(cd);
            }

            Admin admin = hConnection.getAdmin();

            if (regionSplits == null)
                admin.createTable(tableDesc);
            else
                admin.createTable(tableDesc, regionSplits);

        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(String.format("create hbase table:%s failed.", tableName), e);
        }
    }

    public void deleteTable(String tableName) {

        try {
            TableName t = TableName.valueOf(tableName);


            Admin admin = hConnection.getAdmin();

            if (!admin.tableExists(t)) {
                throw new RuntimeException(String.format("hbase table :%s does not exist.", tableName));
            } else {
                admin.disableTable(t);
                admin.deleteTable(t);
            }
        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException(String.format("delete hbase table:%s failed.", tableName), e);
        }

    }

    public boolean existsTable(String tableName) {
        try {
            Admin admin = hConnection.getAdmin();
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException("exist table failure!", e);
        }

    }

    public static byte[][] getDefaultRegions(int regions) {

        if (regions % 2 != 0) {
            throw new RuntimeException("the regions number must be Divisible by 2");
        }

        byte[][] splitKeys = new byte[regions - 1][];

        int bits = (int) (Math.log(regions) / Math.log(2));

        int length = (bits / 8 + 1);
        for (int i = 1; i < regions; i++) {

            int tmp = i << (length * 8 - bits);

            splitKeys[i - 1] = Bytes.copy(Bytes.toBytes(tmp), 4 - length, length);
        }

        return splitKeys;
    }

    public boolean existsFamilies(String name, Set<byte[]> columnFamilies) {
        HTable htable = null;
        try {
            htable = getTable(name);
            Set<byte[]> families = htable.getTableDescriptor().getFamiliesKeys();

            for (byte[] family : columnFamilies) {
                if (!families.contains(family)) {
                    return false;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("check hbase table:%s failed", name), e);
        } finally {
            if (htable != null)
                try {
                    htable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
        return true;
    }

}
