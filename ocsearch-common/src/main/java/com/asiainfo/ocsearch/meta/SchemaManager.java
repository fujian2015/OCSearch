package com.asiainfo.ocsearch.meta;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by mac on 2017/4/1.
 */
public class SchemaManager {

    private static Map<String, Schema> schemaMap = new ConcurrentHashMap<String, Schema>();
    private static Map<String, String> tableMap = new ConcurrentHashMap<String, String>();

    private static ReadWriteLock lock = new ReentrantReadWriteLock(false);

    public static boolean existsConfig(String configName) {

        return schemaMap.containsKey(configName);
    }

    /**
     * overwrite schema
     *
     * @param name
     * @param schema
     */
    public static void addSchema(String name, Schema schema) {

        lock.writeLock().lock();

        schemaMap.put(name, schema);

        lock.writeLock().unlock();
    }

    public static boolean removeSchema(String schema) {
        boolean isSuccess = false;
        lock.writeLock().lock();
        if (schemaMap.containsKey(schema)) {
            if (!tableMap.values().contains(schema)) {
                schemaMap.remove(schema);
                isSuccess = true;
            }
        }
        lock.writeLock().unlock();
        return isSuccess;
    }

    public static Schema getSchemaBySchema(String name) {

        lock.readLock().lock();

        Schema schema = schemaMap.get(name);

        lock.readLock().unlock();
        return schema;
    }

    public static Schema getSchemaByTable(String table) {

        lock.readLock().lock();

        Schema schema = schemaMap.get(tableMap.get(table));

        lock.readLock().unlock();

        return schema;
    }

    public static boolean addTable(String table, String schema) {
        boolean isSuccess = false;
        lock.writeLock().lock();

        if (schemaMap.containsKey(schema)) {

            tableMap.put(table, schema);

            isSuccess = true;

        }
        lock.writeLock().unlock();

        return isSuccess;
    }

    public static boolean removeTable(String table) {

        boolean isSuccess = false;
        lock.writeLock().lock();

        if (tableMap.containsKey(table)) {
            tableMap.remove(table);
            isSuccess = true;
        }
        lock.writeLock().unlock();

        return isSuccess;
    }

    public static boolean containsTable(String table) {

        boolean exists = false;

        lock.readLock().lock();

        exists = tableMap.containsKey(table);

        lock.readLock().unlock();
        return exists;
    }

    public static void reset() {

        lock.writeLock().lock();

        tableMap.clear();
        schemaMap.clear();

        lock.writeLock().unlock();
    }

    public static Set<String> schemaNames(){
        return schemaMap.keySet();
    }

    public static Set<String> tableNames(){
        return tableMap.keySet();
    }

}
