package com.asiainfo.ocsearch.metahelper;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.meta.Table;
import org.apache.log4j.Logger;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by mac on 2017/5/11.
 */
public class MetaDataHelper implements OnReconnect {

    static Logger log = Logger.getLogger(MetaDataHelper.class);

    private final String schemaPath;
    private final String tablePath;

    private SolrZkClient zkclient;

    Watcher schemaWatcher = new SchemaWatcher();

    Watcher tableWatcher = new TableWatcher();

    private Map<String, Schema> schemaMap;
    private Map<String, String> tableMap;
    private ReadWriteLock lock = new ReentrantReadWriteLock(false);

    public MetaDataHelper(Properties properties) throws Exception {

        this.schemaPath = properties.getProperty("zookeeper.schema.dir");
        this.tablePath = properties.getProperty("zookeeper.table.dir");

        assert this.schemaPath != null;
        assert this.tablePath != null;

        String serverAddress = properties.getProperty("zookeeper.address", "localhost:2181");
        int timeout = Integer.parseInt(properties.getProperty("zookeeper.timeout", "10000"));
        int connectTimeout = Integer.parseInt(properties.getProperty("zookeeper.connect.timeout", "60000"));

        this.zkclient = new SolrZkClient(serverAddress, timeout, connectTimeout, this);
        load();
    }

//    private final Set<EventListener> listeners =
//            Collections.newSetFromMap(new IdentityHashMap<EventListener, Boolean>());

    private JsonNode readData(String path) throws KeeperException, InterruptedException, IOException {

        byte data[] = zkclient.getData(path, null, null, true);
        return new ObjectMapper().readTree(data);
    }

    @Override
    public void command() {
        reload();
    }

    private void reload() {
        reloadSchemas();
        reloadTables();
    }

    private void reloadTables() {
        log.info("reload tables start...");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Map<String, String> tables = loadTables();
                lock.writeLock().lock();
                this.tableMap = tables;
                lock.writeLock().unlock();
            } catch (KeeperException e) {
                log.error(e);
                continue;
            } catch (InterruptedException e) {
                log.error(e);
            }
            break;
        }
        log.info("reload tables successful.");
    }

    private void reloadSchemas() {
        log.info("reload schemas start...");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Map<String, Schema> schemas = loadSchemas();
                lock.writeLock().lock();
                this.schemaMap = schemas;
                lock.writeLock().unlock();
            } catch (KeeperException e) {
                log.error(e);
                continue;
            } catch (InterruptedException e) {
                log.error(e);
            }
            break;
        }
        log.info("reload schemas successful.");
    }


    private void load() throws Exception {
        log.info("load schemas and tables start!");
        lock.writeLock().lock();
        try {
            this.schemaMap = loadSchemas();
            this.tableMap = loadTables();

        } catch (Exception e) {
            log.error(e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
        log.info("load schemas and tables over!");
    }

    private Map<String, Schema> loadSchemas() throws KeeperException, InterruptedException {

        if (!this.zkclient.exists(this.schemaPath, true))
            this.zkclient.makePath(schemaPath, CreateMode.PERSISTENT, true);

        Map<String, Schema> schemaMap = new ConcurrentHashMap();

        List<String> schemas = this.zkclient.getChildren(schemaPath, schemaWatcher, true);
        for (String name : schemas) {
            String path = this.schemaPath + "/" + name;
            try {
                schemaMap.put(name, new Schema(readData(path)));
            } catch (IOException | ServiceException e) {
                log.error("illegal json data:" + path, e);
            }
        }
        return schemaMap;
    }

    private Map<String, String> loadTables() throws KeeperException, InterruptedException {

        if (null == this.zkclient.exists(this.tablePath, true))
            this.zkclient.makePath(this.tablePath, CreateMode.PERSISTENT, true);

        Map<String, String> tableMap = new ConcurrentHashMap();

        List<String> schemas = this.zkclient.getChildren(this.tablePath, this.tableWatcher, true);

        for (String name : schemas) {
            String path = this.tablePath + "/" + name;
            try {
                tableMap.put(name, new Table(readData(path)).getSchema());
            } catch (IOException | ServiceException e) {
                log.error("illegal json data:" + path, e);
            }
        }
        return tableMap;
    }

    private void addSchema(String name, Schema schema) {
        log.info("add schema " + name);
        lock.writeLock().lock();

        schemaMap.put(name, schema);

        lock.writeLock().unlock();
    }

    private void removeSchema(String schema) {
        log.info("remove schema " + schema);
        lock.writeLock().lock();

        schemaMap.remove(schema);

        lock.writeLock().unlock();
    }

    private void addTable(String table, String schema) {
        log.info("add table " + table);
        lock.writeLock().lock();
        tableMap.put(table, schema);
        lock.writeLock().unlock();
    }

    private void removeTable(String table) {
        log.info("remove table " + table);
        lock.writeLock().lock();
        tableMap.remove(table);
        lock.writeLock().unlock();
    }

    public Schema getSchemaBySchema(String name) {
        Schema schema;
        lock.readLock().lock();
        schema = schemaMap.get(name);
        lock.readLock().unlock();
        return schema;
    }


    public void createSchema(Schema schema) throws Exception {

        String path = this.schemaPath + "/" + schema.getName();
        try {
            if (false == zkclient.exists(path, true)) {
                zkclient.create(path, schema.toString().getBytes("UTF-8"), CreateMode.PERSISTENT, true);
            } else {
                throw new ServiceException("the schema exists at zookeeper!", ErrorCode.SCHEMA_EXIST);
            }
        } catch (Exception e) {
            log.error(e);
            throw e;
        }
    }

    public void createTable(Table table) throws Exception {
        String path = this.tablePath + "/" + table.getName();
        try {
            if (false == zkclient.exists(path, true)) {
                zkclient.create(path, table.toString().getBytes("UTF-8"), CreateMode.PERSISTENT, true);
            } else {
                throw new ServiceException("the table exists at zookeeper!", ErrorCode.SCHEMA_EXIST);
            }
        } catch (Exception e) {
            log.error(e);
            throw e;
        }
    }

    public void deleteTable(String table) throws Exception {
        String path = this.tablePath + "/" + table;
        try {
            if (true == zkclient.exists(path, true)) {
                zkclient.delete(path, -1, true);
            } else {
                throw new ServiceException("the table does not exist at zookeeper!", ErrorCode.SCHEMA_EXIST);
            }
        } catch (Exception e) {
            log.error(e);
            throw e;
        }
    }

    public void deleteSchema(String schema) throws Exception {
        String path = this.schemaPath + "/" + schema;
        try {
            if (true == zkclient.exists(path, true)) {
                zkclient.delete(path, -1, true);
            }
        } catch (Exception e) {
            log.error(e);
            throw e;
        }
    }

    public boolean hasTable(String name) {
        boolean hasTbable;
        lock.readLock().lock();
        hasTbable = tableMap.containsKey(name);
        lock.readLock().unlock();
        return hasTbable;
    }

    public boolean hasSchema(String name) {
        boolean hasSchema;
        lock.readLock().lock();
        hasSchema = schemaMap.containsKey(name);
        lock.readLock().unlock();
        return hasSchema;
    }

    public Schema getSchemaByTable(String name) {
        Schema schema;
        lock.readLock().lock();
        String schemaName = tableMap.get(name);
        schema = schemaMap.get(schemaName);
        lock.readLock().unlock();
        return schema;
    }

    public boolean schemaInUse(String name) {
        boolean isInUse;
        lock.readLock().lock();
        isInUse = tableMap.values().contains(name);
        lock.readLock().unlock();
        return isInUse;
    }


    /**
     * watcher for schema path
     */
    class SchemaWatcher implements Watcher {
        private Lock reloadLock = new ReentrantLock();

        @Override
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                reloadLock.lock();
                reloadSchemas();
                reloadLock.unlock();
            }
        }
    }

    /**
     * watcher for table path
     */
    class TableWatcher implements Watcher {
        private Lock reloadLock = new ReentrantLock();

        @Override
        public void process(WatchedEvent event) {
            log.info(event);
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                reloadLock.lock();
                reloadTables();
                reloadLock.unlock();
            }
        }
    }
}
