package com.asiainfo.ocsearch.flume.sink;

import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.expression.Engine;
import com.asiainfo.ocsearch.expression.Executor;
import com.asiainfo.ocsearch.flume.util.HttpRestFulClient;
import com.asiainfo.ocsearch.meta.Schema;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.PrivilegedExceptionAction;
import java.util.*;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.hbase.BatchAware;
import org.apache.flume.sink.hbase.HBaseSinkConfigurationConstants;
import org.apache.flume.sink.hbase.HBaseSinkSecurityManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Aaron on 17/6/27.
 */
public class OCSearchHbaseSink extends AbstractSink implements Configurable {

    private String tableName;
    private byte[] columnFamily;
    private HTable table;
    private long batchSize;
    private Configuration config;
    private static final Logger logger = LoggerFactory.getLogger(OCSearchHbaseSink.class);
    private OCHbaseEventSerializer serializer;
    private String eventSerializerType;
    private Context serializerContext;
    private String kerberosPrincipal;
    private String kerberosKeytab;
    private User hbaseUser;
    private boolean enableWal;
    private boolean batchIncrements;
    private Method refGetFamilyMap;
    private SinkCounter sinkCounter;
    private OCSearchHbaseSink.DebugIncrementsCallback debugIncrCallback;

    private Connection connection;
//    private String schemaName;
//    private String tableExpression;
//    private String baseUrl;
//    private String targetUrl;
//    private Schema schema;
//    private Engine expressionEngine = Engine.getInstance();
//    private Executor executor;

    public OCSearchHbaseSink() {
        this(HBaseConfiguration.create());
    }

    public OCSearchHbaseSink(Configuration conf) {
        this.enableWal = true;
        this.batchIncrements = false;
        this.refGetFamilyMap = null;
        this.debugIncrCallback = null;
        this.config = conf;
        logger.info("!!!config is "+this.config.get("hbase-site.xml"));
    }

    @VisibleForTesting
    @InterfaceAudience.Private
    OCSearchHbaseSink(Configuration conf, OCSearchHbaseSink.DebugIncrementsCallback cb) {
        this(conf);
        this.debugIncrCallback = cb;
    }

    public void start() {
//        Preconditions.checkArgument(this.table == null, "Please call stop before calling start on an old instance.");

        try {
            if(HBaseSinkSecurityManager.isSecurityEnabled(this.config)) {
                this.hbaseUser = HBaseSinkSecurityManager.login(this.config, (String)null, this.kerberosPrincipal, this.kerberosKeytab);
            }
        } catch (Exception var4) {
            this.sinkCounter.incrementConnectionFailedCount();
            throw new FlumeException("Failed to login to HBase using provided credentials.", var4);
        }

        try {

            this.connection = (Connection)this.runPrivileged(() ->
            {Connection c = ConnectionFactory.createConnection(this.config);
            return c;});
            if(connection!=null) {
                logger.info("connection!!create");
            }
        } catch (Exception var2) {
            this.sinkCounter.incrementConnectionFailedCount();
            logger.error("Could not get connection from HBase", var2);
            throw new FlumeException("Could not get connection from HBase", var2);
        }

//        try {
//            this.schema = getSchema();
//            this.tableExpression = this.schema.getTableExpression();
//        } catch (Exception e) {
//            this.sinkCounter.incrementConnectionFailedCount();
//            logger.error("Could not get schema from ocsearch server", e);
//            throw new FlumeException("Could not get schema from ocsearch server", e);
//        }
//        executor = expressionEngine.createExecutor(tableExpression);

        //// TODO: 17/6/29 验证cf是否存在,需要从schema中获取cf值,并验证.


        super.start();
        this.sinkCounter.incrementConnectionCreatedCount();
        this.sinkCounter.start();
    }

    public void stop() {
        try {
//            if(this.table != null) {
//                this.table.close();
//            }
            if(this.connection != null) {
                this.connection.close();
            }

//            this.table = null;
            this.connection = null;
        } catch (IOException var2) {
            throw new FlumeException("Error closing table.", var2);
        }

        this.sinkCounter.incrementConnectionClosedCount();
        this.sinkCounter.stop();
    }

    public void configure(Context context) {
//        this.schemaName = context.getString("schema");
//        this.baseUrl = context.getString("OCSearchServer");
//        this.targetUrl = this.baseUrl + "/schema/get?type=schema&name=" + this.schemaName;
//        this.tableName = context.getString("table");
//        String cf = context.getString("columnFamily");
        this.batchSize = context.getLong("batchSize", new Long(100L)).longValue();
        this.serializerContext = new Context();
        this.eventSerializerType = context.getString("serializer");
//        Preconditions.checkNotNull(this.tableName, "Table name cannot be empty, please specify in configuration file");
//        Preconditions.checkNotNull(cf, "Column family cannot be empty, please specify in configuration file");
        if(this.eventSerializerType == null || this.eventSerializerType.isEmpty()) {
            this.eventSerializerType = "org.apache.flume.sink.hbase.SimpleHbaseEventSerializer";
            logger.info("No serializer defined, Will use default");
        }

        this.serializerContext.putAll(context.getSubProperties("serializer."));
//        this.serializerContext.put("schema",this.schemaName);
//        this.columnFamily = cf.getBytes(Charsets.UTF_8);

        try {
            Class zkQuorum = Class.forName(this.eventSerializerType);
            this.serializer = (OCHbaseEventSerializer)zkQuorum.newInstance();
            this.serializer.configure(this.serializerContext);
        } catch (Exception var10) {
            logger.error("Could not instantiate event serializer.", var10);
            Throwables.propagate(var10);
        }

        this.kerberosKeytab = context.getString("kerberosKeytab", "");
        this.kerberosPrincipal = context.getString("kerberosPrincipal", "");
        this.enableWal = context.getBoolean("enableWal", Boolean.valueOf(true)).booleanValue();
        logger.info("The write to WAL option is set to: " + String.valueOf(this.enableWal));
        if(!this.enableWal) {
            logger.warn("HBase Sink\'s enableWal configuration is set to false. All writes to HBase will have WAL disabled, and any data in the memstore of this region in the Region Server could be lost!");
        }

        this.batchIncrements = context.getBoolean("coalesceIncrements", HBaseSinkConfigurationConstants.DEFAULT_COALESCE_INCREMENTS).booleanValue();
        if(this.batchIncrements) {
            logger.info("Increment coalescing is enabled. Increments will be buffered.");
            this.refGetFamilyMap = reflectLookupGetFamilyMap();
        }

        String var11 = context.getString("zookeeperQuorum");
        Integer port = null;
        if(var11 != null && !var11.isEmpty()) {
            StringBuilder hbaseZnode = new StringBuilder();
            logger.info("Using ZK Quorum: " + var11);
            String[] zkHosts = var11.split(",");
            int length = zkHosts.length;

            for(int i = 0; i < length; ++i) {
                String[] zkHostAndPort = zkHosts[i].split(":");
                hbaseZnode.append(zkHostAndPort[0].trim());
                if(i != length - 1) {
                    hbaseZnode.append(",");
                } else {
                    var11 = hbaseZnode.toString();
                }

                if(zkHostAndPort[1] == null) {
                    throw new FlumeException("Expected client port for the ZK node!");
                }

                if(port == null) {
                    port = Integer.valueOf(Integer.parseInt(zkHostAndPort[1].trim()));
                } else if(!port.equals(Integer.valueOf(Integer.parseInt(zkHostAndPort[1].trim())))) {
                    throw new FlumeException("All Zookeeper nodes in the quorum must use the same client port.");
                }
            }

            if(port == null) {
                port = Integer.valueOf(2181);
            }

            this.config.set("hbase.zookeeper.quorum", var11);
            this.config.setInt("hbase.zookeeper.property.clientPort", port.intValue());
        }

        String var12 = context.getString("znodeParent");
        if(var12 != null && !var12.isEmpty()) {
            this.config.set("zookeeper.znode.parent", var12);
        }

        this.sinkCounter = new SinkCounter(this.getName());
    }

    public Configuration getConfig() {
        return this.config;
    }

    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = this.getChannel();
        Transaction txn = channel.getTransaction();
        Map<String,List<Row>> actionMap = new HashMap<>();
        Map<String,List<Increment>> incMap = new HashMap<>();

        try {
            txn.begin();
            if(this.serializer instanceof BatchAware) {
                ((BatchAware)this.serializer).onBatchStart();
            }

            long e;
            for(e = 0L; e < this.batchSize; ++e) {
                Event event = channel.take();
                if(event == null) {
                    if(e == 0L) {
                        status = Status.BACKOFF;
                        this.sinkCounter.incrementBatchEmptyCount();
                    } else {
                        this.sinkCounter.incrementBatchUnderflowCount();
                    }
                    break;
                }

                this.serializer.initialize(event);
//                logger.info("event header is "+event.getHeaders().get("file"));
                actionMap = addMap(actionMap,this.serializer.getActions());
                incMap = addMap(incMap,this.serializer.getIncrements());
            }

            if(e == this.batchSize) {
                this.sinkCounter.incrementBatchCompleteCount();
            }

            this.sinkCounter.addToEventDrainAttemptCount(e);
            this.putEventsAndCommit(actionMap, incMap, txn);
        } catch (Throwable var14) {
            try {
                txn.rollback();
            } catch (Exception var13) {
                logger.error("Exception in rollback. Rollback might not have been successful.", var13);
            }

            logger.error("Failed to commit transaction.Transaction rolled back.", var14);
            if(!(var14 instanceof Error) && !(var14 instanceof RuntimeException)) {
                logger.error("Failed to commit transaction.Transaction rolled back.", var14);
                throw new EventDeliveryException("Failed to commit transaction.Transaction rolled back.", var14);
            }

            logger.error("Failed to commit transaction.Transaction rolled back.", var14);
            Throwables.propagate(var14);
        } finally {
            txn.close();
        }

        return status;
    }
    private void putEventsAndCommit(final Map<String,List<Row>> actionsMap, final Map<String,List<Increment>> incMap, Transaction txn) throws Exception {

        this.runPrivileged(new PrivilegedExceptionAction() {
            public Void run() throws Exception {

                for(Map.Entry<String,List<Row>> entry: actionsMap.entrySet()) {
                    List<Row> actions = entry.getValue();
                    String tableName = entry.getKey();
                    Iterator i$ = actions.iterator();

                    while(i$.hasNext()) {
                        Row r = (Row)i$.next();
                        if(r instanceof Put) {
                            ((Put)r).setDurability(Durability.USE_DEFAULT);
                        }

                        if(r instanceof Increment) {
                            ((Increment)r).setDurability(Durability.USE_DEFAULT);
                        }
                    }
                    Table table = OCSearchHbaseSink.this.connection.getTable(TableName.valueOf(tableName));
//                    String[] result = new String[0];
                    logger.info("HTable "+tableName+" is :"+table);
                    Object[] results = new Object[actions.size()];
//                    logger.info("table name is "+tableName);
//                    logger.info("actionsMap is "+actionsMap);
                    try {
                        table.batch(actions,results);
                    }catch (Exception e) {
                        logger.error(e.toString());
                    }
                    logger.info("actions result is "+results);
                    table.close();
                }
                return null;
            }
        });
        this.runPrivileged(new PrivilegedExceptionAction() {
            public Void run() throws Exception {

                for(Map.Entry<String,List<Increment>> entry: incMap.entrySet()) {
                    List<Increment> incs = entry.getValue();
                    String tableName = entry.getKey();
                    List processedIncrements;
                    if(OCSearchHbaseSink.this.batchIncrements) {
                        processedIncrements = OCSearchHbaseSink.this.coalesceIncrements(incs);
                    } else {
                        processedIncrements = incs;
                    }

                    if(OCSearchHbaseSink.this.debugIncrCallback != null) {
                        OCSearchHbaseSink.this.debugIncrCallback.onAfterCoalesce(processedIncrements);
                    }

                    Iterator i$ = processedIncrements.iterator();

                    while(i$.hasNext()) {
                        Increment i = (Increment)i$.next();
                        i.setDurability(Durability.USE_DEFAULT);
                        Table table = OCSearchHbaseSink.this.connection.getTable(TableName.valueOf(tableName));
                        table.increment(i);
                        table.close();
                    }
                }
                return null;
            }
        });
        txn.commit();
        long size = 0L;
        for(List<Row> value : actionsMap.values()) {
            size += value.size();
        }
        this.sinkCounter.addToEventDrainSuccessCount(size);
    }

//    private void putEventsAndCommit(final List<Row> actions, final List<Increment> incs, Transaction txn) throws Exception {
//        this.runPrivileged(new PrivilegedExceptionAction() {
//            public Void run() throws Exception {
//                Iterator i$ = actions.iterator();
//
//                while(i$.hasNext()) {
//                    Row r = (Row)i$.next();
//                    if(r instanceof Put) {
//                        ((Put)r).setDurability(Durability.USE_DEFAULT);
//                    }
//
//                    if(r instanceof Increment) {
//                        ((Increment)r).setDurability(Durability.USE_DEFAULT);
//                    }
//                }
//
//                OCSearchHbaseSink.this.table.batch(actions);
//                return null;
//            }
//        });
//        this.runPrivileged(new PrivilegedExceptionAction() {
//            public Void run() throws Exception {
//                List processedIncrements;
//                if(OCSearchHbaseSink.this.batchIncrements) {
//                    processedIncrements = OCSearchHbaseSink.this.coalesceIncrements(incs);
//                } else {
//                    processedIncrements = incs;
//                }
//
//                if(OCSearchHbaseSink.this.debugIncrCallback != null) {
//                    OCSearchHbaseSink.this.debugIncrCallback.onAfterCoalesce(processedIncrements);
//                }
//
//                Iterator i$ = processedIncrements.iterator();
//
//                while(i$.hasNext()) {
//                    Increment i = (Increment)i$.next();
//                    i.setDurability(Durability.USE_DEFAULT);
//                    OCSearchHbaseSink.this.table.increment(i);
//                }
//
//                return null;
//            }
//        });
//        txn.commit();
//        this.sinkCounter.addToEventDrainSuccessCount((long)actions.size());
//    }

    private <T> T runPrivileged(PrivilegedExceptionAction<T> action) throws Exception {
        if(this.hbaseUser != null) {
            if(logger.isDebugEnabled()) {
                logger.debug("Calling runAs as hbase user: " + this.hbaseUser.getName());
            }

            return this.hbaseUser.runAs(action);
        } else {
            return action.run();
        }
    }

    @VisibleForTesting
    static Method reflectLookupGetFamilyMap() {
        Method m = null;
        String[] methodNames = new String[]{"getFamilyMapOfLongs", "getFamilyMap"};
        String[] arr$ = methodNames;
        int len$ = methodNames.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            String methodName = arr$[i$];

            try {
                m = Increment.class.getMethod(methodName, new Class[0]);
                if(m != null && m.getReturnType().equals(Map.class)) {
                    logger.debug("Using Increment.{} for coalesce", methodName);
                    break;
                }
            } catch (NoSuchMethodException var7) {
                logger.debug("Increment.{} does not exist. Exception follows.", methodName, var7);
            } catch (SecurityException var8) {
                logger.debug("No access to Increment.{}; Exception follows.", methodName, var8);
            }
        }

        if(m == null) {
            throw new UnsupportedOperationException("Cannot find Increment.getFamilyMap()");
        } else {
            return m;
        }
    }

    private Map<byte[], NavigableMap<byte[], Long>> getFamilyMap(Increment inc) {
        Preconditions.checkNotNull(this.refGetFamilyMap, "Increment.getFamilymap() not found");
        Preconditions.checkNotNull(inc, "Increment required");
        Map familyMap = null;

        try {
            Object e = this.refGetFamilyMap.invoke(inc, new Object[0]);
            familyMap = (Map)e;
        } catch (IllegalAccessException var4) {
            logger.warn("Unexpected error calling getFamilyMap()", var4);
            Throwables.propagate(var4);
        } catch (InvocationTargetException var5) {
            logger.warn("Unexpected error calling getFamilyMap()", var5);
            Throwables.propagate(var5);
        }

        return familyMap;
    }

    private List<Increment> coalesceIncrements(Iterable<Increment> incs) {
        Preconditions.checkNotNull(incs, "List of Increments must not be null");
        TreeMap counters = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        Iterator coalesced = incs.iterator();

        while(coalesced.hasNext()) {
            Increment i$ = (Increment)coalesced.next();
            byte[] rowEntry = i$.getRow();
            Map row = this.getFamilyMap(i$);
            Iterator families = row.entrySet().iterator();

            while(families.hasNext()) {
                Map.Entry inc = (Map.Entry)families.next();
                byte[] i$1 = (byte[])inc.getKey();
                NavigableMap familyEntry = (NavigableMap)inc.getValue();
                Iterator family = familyEntry.entrySet().iterator();

                while(family.hasNext()) {
                    Map.Entry qualifiers = (Map.Entry)family.next();
                    byte[] i$2 = (byte[])qualifiers.getKey();
                    Long qualifierEntry = (Long)qualifiers.getValue();
                    this.incrementCounter(counters, rowEntry, i$1, i$2, qualifierEntry);
                }
            }
        }

        LinkedList coalesced1 = Lists.newLinkedList();
        Iterator i$3 = counters.entrySet().iterator();

        while(i$3.hasNext()) {
            Map.Entry rowEntry1 = (Map.Entry)i$3.next();
            byte[] row1 = (byte[])rowEntry1.getKey();
            Map families1 = (Map)rowEntry1.getValue();
            Increment inc1 = new Increment(row1);
            Iterator i$4 = families1.entrySet().iterator();

            while(i$4.hasNext()) {
                Map.Entry familyEntry1 = (Map.Entry)i$4.next();
                byte[] family1 = (byte[])familyEntry1.getKey();
                NavigableMap qualifiers1 = (NavigableMap)familyEntry1.getValue();
                Iterator i$5 = qualifiers1.entrySet().iterator();

                while(i$5.hasNext()) {
                    Map.Entry qualifierEntry1 = (Map.Entry)i$5.next();
                    byte[] qualifier = (byte[])qualifierEntry1.getKey();
                    long count = ((Long)qualifierEntry1.getValue()).longValue();
                    inc1.addColumn(family1, qualifier, count);
                }
            }

            coalesced1.add(inc1);
        }

        return coalesced1;
    }

    private void incrementCounter(Map<byte[], Map<byte[], NavigableMap<byte[], Long>>> counters, byte[] row, byte[] family, byte[] qualifier, Long count) {
        Object families = (Map)counters.get(row);
        if(families == null) {
            families = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            counters.put(row, (Map<byte[], NavigableMap<byte[], Long>>) families);
        }

        Object qualifiers = (NavigableMap)((Map)families).get(family);
        if(qualifiers == null) {
            qualifiers = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
            ((Map)families).put(family, qualifiers);
        }

        Long existingValue = (Long)((NavigableMap)qualifiers).get(qualifier);
        if(existingValue == null) {
            ((NavigableMap)qualifiers).put(qualifier, count);
        } else {
            ((NavigableMap)qualifiers).put(qualifier, Long.valueOf(existingValue.longValue() + count.longValue()));
        }

    }

    @VisibleForTesting
    @InterfaceAudience.Private
    OCHbaseEventSerializer getSerializer() {
        return this.serializer;
    }

    @VisibleForTesting
    @InterfaceAudience.Private
    interface DebugIncrementsCallback {
        void onAfterCoalesce(Iterable<Increment> var1);
    }

    private <T> Map<String,List<T>> addMap(Map<String,List<T>> batchMap,Map<String,List<T>> actionMap) {

        if(actionMap == null) {
            return batchMap;
        }

        for(Map.Entry<String,List<T>> entry : actionMap.entrySet()) {
            String key = entry.getKey();
            List<T> value = entry.getValue();
            logger.info("++++action list is "+value);
            if(batchMap.containsKey(key)) {
                List<T> batchValue = batchMap.get(key);
                batchValue.addAll(value);
                batchMap.put(key,batchValue);
            }else {
                batchMap.put(key,value);
            }
        }
        return batchMap;
    }

}
