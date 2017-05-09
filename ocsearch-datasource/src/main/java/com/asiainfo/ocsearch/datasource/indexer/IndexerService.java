package com.asiainfo.ocsearch.datasource.indexer;

import com.ngdata.hbaseindexer.ConfKeys;
import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.conf.DefaultIndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactoryUtil;
import com.ngdata.hbaseindexer.model.api.*;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.hbaseindexer.util.zookeeper.StateWatchingZooKeeper;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.ngdata.sep.util.zookeeper.ZooKeeperOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mac on 2017/5/3.
 */
public class IndexerService {

    Logger logger = Logger.getLogger(this.getClass());

    final String zkConnectString;
    final String solrZkConnectString;


    protected Configuration conf;
    protected ZooKeeperItf zk;
    protected WriteableIndexerModel model;

    public IndexerService(String solrZkConnectString) throws Exception {

        try {
            this.solrZkConnectString = solrZkConnectString;

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
     * @param confXml conf xml  absolute path
     */
    public void createTable(String table, String confXml) throws IndexerModelException, IndexerValidityException, IndexerExistsException {

        IndexerDefinition indexer = null;
        try {
            IndexerDefinitionBuilder builder = buildIndexerDefinition(table, confXml, null);
            indexer = builder.build();
        } catch (Exception e) {
            logger.error(e);
        }
        model.addIndexer(indexer);
    }

    /**
     * @param table
     */
    public void deleteTable(String table) throws IndexerModelException {
        model.deleteIndexerInternal(table);
    }

    public boolean exists(String table) {

        return model.hasIndexer(table);
    }

    public void close() {
        Closer.close(model);
        Closer.close(zk);
    }

    private IndexerDefinitionBuilder buildIndexerDefinition(String indexerName, String confXml, IndexerDefinition oldIndexerDef)
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

        byte[] indexerConf = getIndexerConf(indexerName, confXml);

        if (indexerConf != null)
            builder.configuration(indexerConf);

        IndexerComponentFactoryUtil.getComponentFactory(defaultFactory, new ByteArrayInputStream(indexerConf), connectionParams);

        try {
            System.out.println(new SAXReader().read(new ByteArrayInputStream(indexerConf)).asXML());
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        return builder;
    }

    private Map<String, String> getConnectionParams(String indexerName) {

        Map<String, String> paramas = new HashMap<>();
        paramas.put("solr.zk", solrZkConnectString);
        paramas.put("solr.collection", indexerName);
        return paramas;
    }


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
}
