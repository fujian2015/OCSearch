package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.db.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
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

    final String table;
    final String solrCollection;
    final String config;
    final String hbaseTable;


    public CreateIndexerTable(String table, String config,String solrCollection,  String hbaseTable) {

        this.table = table;
        this.solrCollection = solrCollection;
        this.config = config;
        this.hbaseTable = hbaseTable;
    }

    @Override
    public boolean execute() {

        try {
            File indexerFile = generateIndexerFile();

            IndexerServiceManager.getIndexerService().createTable(table, indexerFile.getAbsolutePath(), solrCollection);
            indexerFile.delete();

        } catch (IOException e) {
            throw new RuntimeException("create habse-indexer table " + table + " failure!", e);
        }
        return true;
    }

    private File generateIndexerFile() throws IOException {

        String configPath = ConfigUtil.getIndexerConfigPath(config);

        Document indexerDoc = DocumentHelper.createDocument();

        Element indexer = indexerDoc.addElement("indexer");

        indexer.addAttribute("table", hbaseTable);
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

        String configPath = ConfigUtil.getIndexerConfigPath(config);

        File indexerFile = new File(configPath, table + "_indexer.xml");
        if (indexerFile.exists())
            indexerFile.delete();

        IndexerServiceManager.getIndexerService().deleteTable(table);

        return true;
    }

    @Override
    public boolean canExecute() {


        return !IndexerServiceManager.getIndexerService().exists(table);
    }
}
