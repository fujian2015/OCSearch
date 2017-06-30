package com.asiainfo.ocsearch.datasource.indexer;

import com.beust.jcommander.internal.Lists;
import com.ngdata.hbaseindexer.ConfKeys;
import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.conf.DefaultIndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactoryUtil;
import com.ngdata.hbaseindexer.model.api.*;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.hbaseindexer.mr.HBaseMapReduceIndexerTool;
import com.ngdata.hbaseindexer.util.zookeeper.StateWatchingZooKeeper;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.ngdata.sep.util.zookeeper.ZooKeeperOperation;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by mac on 2017/5/3.
 */
public class IndexerService {

    Logger logger = Logger.getLogger(this.getClass());

    final String zkConnectString;
    final String solrZkConnectString;
    final String libPath;


    protected Configuration conf;
    protected ZooKeeperItf zk;
    protected WriteableIndexerModel model;

    Configuration batchConf = null;

    public IndexerService(Properties p) throws Exception {

        try {
            this.solrZkConnectString = p.getProperty("solr.zookeeper");
            this.libPath = p.getProperty("indexer.lib.path");

            conf = HBaseIndexerConfiguration.create();
            zkConnectString = conf.get(ConfKeys.ZK_CONNECT_STRING);

            connectWithZooKeeper();
//
            model = new IndexerModelImpl(zk, conf.get(ConfKeys.ZK_ROOT_NODE));
        } catch (Exception e) {
            logger.error(e);
            throw e;
        }
    }

    /**
     * @param table
     * @param indexerConf
     */
    public void createTable(String table, byte[] indexerConf) throws IndexerModelException, IndexerValidityException, IndexerExistsException {

        IndexerDefinition indexer = null;
        try {
            IndexerDefinitionBuilder builder = buildIndexerDefinition(table, indexerConf, null);
            indexer = builder.build();
        } catch (Exception e) {
            logger.error(e);
        }
        model.addIndexer(indexer);
    }

    /**
     * @param indexerName
     */
    public void deleteTable(String indexerName) throws Exception {
        try {
            IndexerDefinition indexerDef = model.getIndexer(indexerName);

            if (indexerDef.getLifecycleState() == IndexerDefinition.LifecycleState.DELETE_REQUESTED
                    || indexerDef.getLifecycleState() == IndexerDefinition.LifecycleState.DELETING) {
                logger.warn(String.format("Delete of '%s' is already in progress", indexerName));
                return;
            }

            IndexerDefinitionBuilder builder = new IndexerDefinitionBuilder();
            builder.startFrom(indexerDef);
            builder.lifecycleState(IndexerDefinition.LifecycleState.DELETE_REQUESTED);

            model.updateIndexerInternal(builder.build());
            waitForDeletion(indexerName);
        } catch (Exception e) {
            throw e;
        }

    }

    private void waitForDeletion(String indexerName) throws InterruptedException, KeeperException {
        logger.info(String.format("Deleting indexer '%s'", indexerName));
        while (model.hasIndexer(indexerName)) {
            IndexerDefinition indexerDef;
            try {
                indexerDef = model.getFreshIndexer(indexerName);
            } catch (IndexerNotFoundException e) {
                // The indexer was deleted between the call to hasIndexer and getIndexer, that's ok
                break;
            }

            switch (indexerDef.getLifecycleState()) {
                case DELETE_FAILED:
                    System.err.println("\nDelete failed");
                    return;
                case DELETE_REQUESTED:
                case DELETING:
                    System.out.print(".");
                    Thread.sleep(500);
                    continue;
                default:
                    throw new IllegalStateException("Illegal lifecycle state while deleting: "
                            + indexerDef.getLifecycleState());
            }
        }
        logger.info(String.format("Deleted indexer '%s'", indexerName));
    }

    public boolean exists(String table) {

        return model.hasIndexer(table);
    }

    public void close() {
        Closer.close(model);
        Closer.close(zk);
    }

    private IndexerDefinitionBuilder buildIndexerDefinition(String indexerName, byte[] indexerConf, IndexerDefinition oldIndexerDef)
            throws IOException {

        IndexerDefinitionBuilder builder = new IndexerDefinitionBuilder();
        if (oldIndexerDef != null)
            builder.startFrom(oldIndexerDef);

        builder.name(indexerName);

        // connection type is a hardcoded setting
        builder.connectionType("solr");

        Map<String, String> connectionParams = getConnectionParams(indexerName);

        builder.connectionParams(connectionParams);

        String defaultFactory = DefaultIndexerComponentFactory.class.getName();

        builder.indexerComponentFactory(defaultFactory);

        if (indexerConf != null)
            builder.configuration(indexerConf);

        IndexerComponentFactoryUtil.getComponentFactory(defaultFactory, new ByteArrayInputStream(indexerConf), connectionParams);

        return builder;
    }

    private Map<String, String> getConnectionParams(String indexerName) {

        Map<String, String> paramas = new HashMap<>();
        paramas.put("solr.zk", solrZkConnectString);
        paramas.put("solr.collection", indexerName);
        return paramas;
    }

    private void connectWithZooKeeper() throws Exception {

        int zkSessionTimeout = HBaseIndexerConfiguration.getSessionTimeout(conf);
        zk = new StateWatchingZooKeeper(zkConnectString, zkSessionTimeout);

        final String zkRoot = conf.get("hbaseindexer.zookeeper.znode.parent");

        boolean indexerNodeExists = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
            @Override
            public Boolean execute() throws KeeperException, InterruptedException {
                return zk.exists(zkRoot, false) != null;
            }
        });

        if (!indexerNodeExists) {
            throw new Exception("WARNING: No " + zkRoot + " node found in ZooKeeper.");
        }
    }

    @Deprecated
    private byte[] getIndexerConf(String name, String confXml) throws IOException {

        Document indexerDoc = DocumentHelper.createDocument();

        Element indexer = indexerDoc.addElement("indexer");

        indexer.addAttribute("table", name);
        indexer.addAttribute("mapper", "com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper");

        Element file = indexer.addElement("param");
        file.addAttribute("name", "morphlineFile");
        file.addAttribute("value", confXml);

        Element isProduct = indexer.addElement("param");
        isProduct.addAttribute("name", "isProductionMode");
        isProduct.addAttribute("value", "true");

        Element morphlineId = indexer.addElement("param");
        morphlineId.addAttribute("name", "morphlineId");
        morphlineId.addAttribute("value", "morphline");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        XMLWriter xmlWriter = null;

        try {
            xmlWriter = new XMLWriter(out);
            xmlWriter.write(indexerDoc);

        } catch (IOException e) {
            throw e;
        } finally {
            if (xmlWriter != null)
                xmlWriter.close();
        }

        return out.toByteArray();
    }

    public int batchIndex(String table, long startTime, long endTime) {

        List<String> args = Lists.newArrayList("--hbase-indexer-zk", zkConnectString, "--hbase-indexer-name", table, "--go-live");

        if (startTime != -1) {
            args.add("--hbase-start-time");
            args.add(String.valueOf(startTime));
        }
        if (endTime != -1) {
            args.add("--hbase-end-time");
            args.add(String.valueOf(endTime));
        }

        String[] a = new String[args.size()];
        try {
            HBaseMapReduceIndexerTool h = new HBaseMapReduceIndexerTool();
            h.setConf(getConfiguration());
            return h.run(args.toArray(a));
        } catch (Exception e) {
            logger.error(e);
            throw new RuntimeException("execute batch index error!", e);
        }
    }

    public Configuration getConfiguration() {
        if (batchConf == null) {
            batchConf = new Configuration(conf);
            batchConf.set("mapreduce.framework.name", "yarn");
            batchConf.addResource("hdfs-site.xml");
            batchConf.addResource("yarn-site.xml");
            batchConf.addResource("mapred-site.xml");
            batchConf.set("mapreduce.job.user.classpath.first", "true", "mapred-site.xml");

            List<String> jars = new ArrayList<>();
            try {
                FileSystem fileSystem = FileSystem.get(batchConf);
                for (FileStatus fs : fileSystem.listStatus(new Path(this.libPath))) {
                    System.out.println(fs.getPath());
                    jars.add(fs.getPath().toString());
                }
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e);
                throw new RuntimeException(e);
            }
            String tmpJars = StringUtils.join(jars, ",");
            batchConf.set("tmpjars", tmpJars);
        }
        return batchConf;
    }
}
