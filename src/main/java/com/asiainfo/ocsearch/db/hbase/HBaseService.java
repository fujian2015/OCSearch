package com.asiainfo.ocsearch.db.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class HBaseService implements Closeable {


    Logger log=Logger.getLogger(getClass());
    private Map<String, HTable> tableMap = new ConcurrentHashMap();

    //多个HTable用同一个connection,同一个pool
    private Connection hConnection;

    public HBaseService(Connection conn) {
        this.hConnection = conn;
    }

    public void close() throws IOException {
        IOException lastError = null;

        for (String tableName : tableMap.keySet()) {
            try {
                Table table = tableMap.get(tableName);
                table.close();
            } catch (IOException e) {
                lastError = e;
            }
        }

        try {
            if (hConnection != null)
                hConnection.close();

        } catch (IOException e) {
            lastError = e;
        }

        if (lastError != null)
            throw lastError;
    }

    public void createTable(String tableName, Set<byte[]> columnFamilies, int regions) {
        try {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

            tableDesc.setDurability(Durability.SKIP_WAL);
            //maxVersions
            //CompressionType
            //setBloomFilterType
            //setBlockCacheEnabled
            for (byte[] columnFamily : columnFamilies) {

                HColumnDescriptor cd = new HColumnDescriptor(columnFamily);

                tableDesc.addFamily(cd);
            }

            Admin admin = hConnection.getAdmin();

            if (admin.tableExists(TableName.valueOf(tableName))) {
                throw new RuntimeException(String.format("hbase table :%s exists.", tableName));
            }

            if (regions == -1) {
                admin.createTable(tableDesc);
            } else {
                admin.createTable(tableDesc, getDefaultRegions(regions));
            }

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

        int bits = (int) (Math.log(regions)/Math.log(2));

        int length = (bits / 8 + 1);
        for (int i = 1; i < regions; i++) {

            int tmp = i << (length * 8 - bits);

            splitKeys[i-1] = Bytes.copy(Bytes.toBytes(tmp),4-length,length);
        }

        return splitKeys;
    }

    private HTable getTable(String tableName) throws IOException {
        synchronized (tableMap) {
            HTable htable = tableMap.get(tableName);

            if (htable != null) {
                return htable;
            } else {
                if (this.hConnection.getAdmin().tableExists(TableName.valueOf(tableName))) {
                    htable = (HTable) this.hConnection.getTable(TableName.valueOf(tableName));
                    if (htable != null) {
                        tableMap.put(tableName, htable);
                    }
                }
            }
            return htable;
        }
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

    public Result getResult(String tableName, final Get get) {

        Result result;
        try {
            result = getTable(tableName).get(get);

        } catch (IOException e) {
            throw new RuntimeException("Exception occured in getResult method, tableName=" + tableName, e);
        }
        return result;

    }

    public byte[] get(String tableName, byte[] row, final byte[] family, final byte[] qualifier) {
        final Get get = new Get(row);
        get.addColumn(family, qualifier);

        Result result = this.getResult(tableName, get);
        if (result.isEmpty())
            return null;
        return result.getValue(family, qualifier);
    }

    public Map<byte[], byte[]> gets(String tableName, byte[] row, final byte[] family, final byte[]... qualifiers) {

        Map<byte[], byte[]> resultMap = new HashMap<>();
        final Get get = new Get(row);
        for (byte[] qualifier : qualifiers) {
            get.addColumn(family, qualifier);
        }

        Result result = this.getResult(tableName, get);
        if (!result.isEmpty()) {
            for (byte[] qualifier : qualifiers) {

                resultMap.put(qualifier,
                        result.getValue(family, qualifier));
            }
        }

        return resultMap;
    }


    public boolean exists(String tableName, final byte[] rowkey) {

        final Get get = new Get(rowkey);

        return this.exists(tableName, get);

    }

    public boolean exists(String tableName, final Get get) {
        try {
            return getTable(tableName).exists(get);
        } catch (IOException e) {
            throw new RuntimeException("Exception occured in exists method, tableName=" + tableName, e);
        }
    }

    public boolean exists(String tableName, byte[] row, byte[] family) {
        Get get = new Get(row);
        get.addFamily(family);
        return exists(tableName, get);
    }

    public boolean exists(String tableName, byte[] row, byte[] family,
                          byte[] qualifier) {
        Get get = new Get(row);
        get.addColumn(family, qualifier);
        return exists(tableName, get);
    }


    public boolean existsFamilies(String name, Set<byte[]> columnFamilies) {
        try {

            HTable htable = tableMap.get(name);
            Set<byte[]> families = htable.getTableDescriptor().getFamiliesKeys();

            for (byte[] family : columnFamilies) {
                if (!families.contains(family)) {
                    return false;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("check hbase table:%s failed", name), e);
        }
        return true;
    }
}
