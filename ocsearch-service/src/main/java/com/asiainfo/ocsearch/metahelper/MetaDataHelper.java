package com.asiainfo.ocsearch.metahelper;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.meta.Table;
import org.apache.commons.lang3.StringUtils;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by mac on 2017/5/11.
 */
public class MetaDataHelper implements OnReconnect {
    private Lock reloadLock = new ReentrantLock();

    static Logger log = Logger.getLogger(MetaDataHelper.class);

    private final String schemaPath;
    private final String tablePath;

    private final String lockPath;

    private SolrZkClient zkclient;

    Watcher schemaWatcher = new SchemaWatcher();

    Watcher tableWatcher = new TableWatcher();

    private Map<String, Schema> schemaMap;
    private Map<String, String> tableMap;
    private Lock lock = new ReentrantLock();

    public MetaDataHelper(Properties properties) throws Exception {

        this.schemaPath = properties.getProperty("zookeeper.schema.dir");
        this.tablePath = properties.getProperty("zookeeper.table.dir");
        this.lockPath = properties.getProperty("zookeeper.lock.dir", "/ocsearch/lock");

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

    private JsonNode readData(String path, Watcher watcher) throws KeeperException, InterruptedException, IOException {

        byte data[] = zkclient.getData(path, watcher, null, true);
        return new ObjectMapper().readTree(data);
    }

    @Override
    public void command() {
        reload();
    }

    private void reload() {
        reloadLock.lock();
        reloadSchemas();
        reloadTables();
        reloadLock.unlock();
    }

    private void reloadTables() {
        log.info("reload tables start...");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Map<String, String> tables = loadTables();
                log.debug(" get lock...");
                lock.lock();
                log.debug("release lock...");
                this.tableMap = tables;
                lock.unlock();
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
                log.debug(" get lock...");
                lock.lock();
                this.schemaMap = schemas;
                lock.unlock();
                log.debug("release lock...");
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
        lock.lock();
        try {
            this.schemaMap = loadSchemas();
            this.tableMap = loadTables();

        } catch (Exception e) {
            log.error(e);
            throw e;
        } finally {
            lock.unlock();
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
                schemaMap.put(name, new Schema(readData(path, schemaWatcher)));
            } catch (IOException | ServiceException e) {
                log.error("illegal json data:" + path, e);
            }
        }
        return schemaMap;
    }

    private Map<String, String> loadTables() throws KeeperException, InterruptedException {

        if (!this.zkclient.exists(this.tablePath, true))
            this.zkclient.makePath(this.tablePath, CreateMode.PERSISTENT, true);

        Map<String, String> tableMap = new ConcurrentHashMap();

        List<String> schemas = this.zkclient.getChildren(this.tablePath, this.tableWatcher, true);

        for (String name : schemas) {
            String path = this.tablePath + "/" + name;
            try {
                tableMap.put(name, new Table(readData(path, null)).getSchema());
            } catch (IOException | ServiceException e) {
                log.error("illegal json data:" + path, e);
            }
        }
        return tableMap;
    }


    public Schema getSchemaBySchema(String name) {
        Schema schema;
        lock.lock();
        try {
            schema = schemaMap.get(name);
        } finally {
            lock.unlock();
        }
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

    public void updateSchema(Schema schema) throws Exception {

        String path = this.schemaPath + "/" + schema.getName();
        try {
            if (true == zkclient.exists(path, true)) {
                zkclient.setData(path, schema.toString().getBytes("UTF-8"), true);
            } else {
                throw new ServiceException("the schema does not exist at zookeeper!", ErrorCode.SCHEMA_NOT_EXIST);
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
        lock.lock();
        try {
            return tableMap.containsKey(name);
        } finally {
            lock.unlock();
        }
    }

    public boolean hasSchema(String name) {
        lock.lock();
        try {
            return schemaMap.containsKey(name);
        } finally {
            lock.unlock();
        }
    }

    public Schema getSchemaByTable(String name) {
        lock.lock();
        try {
            String schemaName = tableMap.get(name);
            return schemaMap.get(schemaName);
        } finally {
            lock.unlock();
        }
    }

    public boolean schemaInUse(String name) {
        lock.lock();
        try {
            return tableMap.values().contains(name);
        } finally {
            lock.unlock();
        }
    }

    public Set<String> getTables() {
        lock.lock();
        try {
            return tableMap.keySet();
        } finally {
            lock.unlock();
        }
    }

    public Set<String> getTablesBySchema(String schema) {
        lock.lock();
        try {
            Set<String> tables = new TreeSet<>();
            for (Map.Entry<String, String> entry : tableMap.entrySet()) {
                if (StringUtils.equals(schema, entry.getValue())) {
                    tables.add(entry.getKey());
                }
            }
            return tables;
        } finally {
            lock.unlock();
        }
    }

    public Collection<Schema> getSchemas() {
        return schemaMap.values();
    }

    public String lock(String s) {
        String lock = this.lockPath + "/" + s;
        try {
            if (!this.zkclient.exists(this.lockPath, true))
                this.zkclient.makePath(lockPath, CreateMode.PERSISTENT, true);

            if (zkclient.exists(lock, true) == false) {

                zkclient.create(lock, null, CreateMode.PERSISTENT, true);
                return lock;
            }
            else {
                log.warn("lock file exists:" + lock);
            }

        } catch (Exception e) {
            log.error("create lock failed:" + lock, e);
        }
        return null;
    }

    public void unlock(String lock) throws Exception {
        try {
            if (zkclient.exists(lock, true) == true) {
                zkclient.delete(lock,-1,true);
            }
        } catch (Exception e) {
            throw new Exception("unlock failed:"+lock,e);
        }
    }


    /**
     * watcher for schema path
     */
    class SchemaWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {

            if (event.getType() == Event.EventType.NodeChildrenChanged || event.getType() == Event.EventType.NodeDataChanged) {
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
