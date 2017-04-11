package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.core.TableSchema;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;

import java.io.*;
import java.util.List;

/**
 * Created by mac on 2017/3/27.
 */
public class GenerateSolrConfig implements AtomicOperation ,Serializable{

    TableSchema tableSchema;

    public GenerateSolrConfig(TableSchema tableSchema) {

        this.tableSchema = tableSchema;
    }

    public boolean execute() {

        String path = ConfigUtil.getSolrConfigPath(tableSchema.name);

        File config = new File(path);

        if (config.exists()) {
            throw new RuntimeException("the work dir exists " + path);
        }

        config.mkdirs();

        File managed_schema = new File(path, "managed-schema");

        File solrconfig = new File(path, "solrconfig.xml");

        FileOutputStream configOut = null;
        FileOutputStream schemaOut = null;

        FileInputStream configIn = null;
        FileInputStream schemaIn = null;

        try {
            configIn = new FileInputStream(PropertiesLoadUtil.loadFile("example_config/solrconfig.xml"));
            schemaIn = new FileInputStream(PropertiesLoadUtil.loadFile("example_config/managed-schema"));

            configOut = new FileOutputStream(solrconfig);
            schemaOut = new FileOutputStream(managed_schema);

            IOUtils.copy(configIn, configOut);
            IOUtils.copy(schemaIn, schemaOut);

        } catch (IOException e) {

            e.printStackTrace();
            throw new RuntimeException("copy config fiels failure", e);
        } finally {
            if (configIn != null) IOUtils.closeQuietly(configIn);
            if (configOut != null) IOUtils.closeQuietly(configOut);
            if (schemaIn != null) IOUtils.closeQuietly(schemaIn);
            if (schemaOut != null) IOUtils.closeQuietly(schemaOut);
        }

        generateSchema(managed_schema);

        return true;
    }

    private void generateSchema(File managed_schema) {
        try {
            SAXReader sr = new SAXReader();

            Document schemaDoc = sr.read(managed_schema);

            Element root = schemaDoc.getRootElement();

            List<Element> fields = tableSchema.getSolrFields();

            for (Element field : fields) {
                root.addText("\n\t");
                root.add(field);

            }
            root.addText("\n");
            XMLWriter xmlWriter = new XMLWriter(new FileWriter(managed_schema));

            xmlWriter.write(schemaDoc);
            xmlWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("generate solr schema failure", e);
        }
    }

    public boolean recovery() {

        String path =  ConfigUtil.getSolrConfigPath(tableSchema.name);

        File dir = new File(path);

        if (dir.exists()) {
            return deleteDir(dir);
        }
        return true;
    }

    @Override
    public boolean canExecute() {

        String path = ConfigUtil.getSolrConfigPath(tableSchema.name);

        File config = new File(path);

        return !config.exists();
    }

    private boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}
