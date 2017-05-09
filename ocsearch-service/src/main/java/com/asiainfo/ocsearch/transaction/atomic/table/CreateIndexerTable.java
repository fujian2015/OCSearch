package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.indexer.IndexerService;
import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import com.ngdata.hbaseindexer.model.api.IndexerModelException;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by mac on 2017/4/6.
 */
public class CreateIndexerTable implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    final String table;

    final String config;

    public CreateIndexerTable(String table, String config) {

        this.table = table;
        this.config = config;
    }

    @Override
    public boolean execute() {

        try {
            log.info("create indexer table " + table + " start!");
            String configPath = ConfigUtil.getIndexerConfigPath(config);
            IndexerServiceManager.getIndexerService().createTable(table, new File(configPath, "morphlines.conf").getAbsolutePath());
            log.info("create indexer table " + table + " success!");
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("create habse-indexer table " + table + " failure!", e);
        }
        return true;
    }

    @Deprecated
    private File generateIndexerFile() throws IOException {

        String configPath = ConfigUtil.getIndexerConfigPath(config);

        Document indexerDoc = DocumentHelper.createDocument();


        Element indexer = indexerDoc.addElement("indexer");

        indexer.addAttribute("table", table);
        indexer.addAttribute("mapper", "com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper");

        Element file = indexer.addElement("param");
        file.addAttribute("name", "morphlineFile");
        file.addAttribute("value", new File(configPath, "morphlines.conf").getAbsolutePath());

        Element isProduct = indexer.addElement("param");
        isProduct.addAttribute("name", "isProductionMode");
        isProduct.addAttribute("value", "true");

        Element morphlineId = indexer.addElement("param");
        morphlineId.addAttribute("name", "morphlineId");
        morphlineId.addAttribute("value", config);

        File indexerFile = new File(configPath, table + "_indexer.xml");
        XMLWriter xmlWriter = null;
        try {
            xmlWriter = new XMLWriter(new FileWriter(indexerFile));
            xmlWriter.write(indexerDoc);

            new XMLWriter();
        } catch (IOException e) {
            throw e;
        } finally {
            if (xmlWriter != null)
                xmlWriter.close();
        }
        return indexerFile;

    }

    @Override
    public boolean recovery() {

        IndexerService indexerService2 = IndexerServiceManager.getIndexerService();

        try {
            log.info("delete hbase table " + table + " start!");
            if (indexerService2.exists(table))
                indexerService2.deleteTable(table);
            log.info("delete hbase table " + table + " success!");
        } catch (IndexerModelException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public boolean canExecute() {
        return !IndexerServiceManager.getIndexerService().exists(table);
    }
}
