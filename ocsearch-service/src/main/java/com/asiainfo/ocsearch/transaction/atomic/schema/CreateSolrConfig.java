package com.asiainfo.ocsearch.transaction.atomic.schema;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.datasource.solr.SolrConfig;
import com.asiainfo.ocsearch.datasource.solr.SolrServer;
import com.asiainfo.ocsearch.datasource.solr.SolrServerManager;
import com.asiainfo.ocsearch.meta.ContentField;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultElement;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mac on 2017/3/27.
 */
public class CreateSolrConfig implements AtomicOperation {

    static Logger log = Logger.getLogger("state");
    Schema tableSchema;

    public CreateSolrConfig(Schema tableSchema) {

        this.tableSchema = tableSchema;
    }

    public boolean execute() {
        String schema = tableSchema.getName();

        log.info("upload  solr config " + schema + " start!");
        String path = ConfigUtil.getSolrConfigPath(schema);

        File config = new File(path);

        if (config.exists()) {
            throw new RuntimeException("the work dir exists " + path);
        }

        try {
           SolrServer server= SolrServerManager.getInstance();
            FileUtils.copyDirectory(new File(PropertiesLoadUtil.loadFile("example_config")), config);

            generateSchema(new File(path, "managed-schema"));

            generateSolrConfig(new File(path, "solrconfig.xml"),server.getSolrConfig());

            server.uploadConfig(Paths.get(path), schema);

        } catch (Exception e) {
            log.error(e);
            throw new RuntimeException("upload config " + path + " failure!", e);
        } finally {
            deleteDir(config);
        }

        log.info("upload  solr config " + schema + " success!");

        return true;
    }

    private void generateSolrConfig(File config, SolrConfig solrConfig) {
        try {

            SAXReader sr = new SAXReader();

            Document configDoc = sr.read(config);

            Element root = configDoc.getRootElement();

            Element factory = root.element("directoryFactory");

            factory.addText("\n");

            Element hdfsHome = factory.addElement("str");

            hdfsHome.addAttribute("name","solr.hdfs.home");

            hdfsHome.setText(solrConfig.getHdfsHome());
            if(solrConfig.useHA()){
                factory.addText("\n");
                Element hdfsConf = factory.addElement("str");

                hdfsConf.addAttribute("name","solr.hdfs.confdir");
                hdfsConf.setText(solrConfig.getHdfsConfdir());
            }

            XMLWriter xmlWriter = new XMLWriter(new OutputStreamWriter(new FileOutputStream(config), Constants.DEFUAT_CHARSET));

            xmlWriter.write(configDoc);
            xmlWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("generate solr config file failure", e);
        }
    }

    private void generateSchema(File managed_schema) {
        try {
            SAXReader sr = new SAXReader();

            Document schemaDoc = sr.read(managed_schema);

            Element root = schemaDoc.getRootElement();

            List<Element> fields = generateSolrFields(tableSchema);

            for (Element field : fields) {
                root.addText("\n\t");
                root.add(field);

            }
            root.addText("\n");

            XMLWriter xmlWriter = new XMLWriter(new OutputStreamWriter(new FileOutputStream(managed_schema), Constants.DEFUAT_CHARSET));

            xmlWriter.write(schemaDoc);
            xmlWriter.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("generate solr schema failure", e);
        }
    }

    public boolean recovery() {

        String schema = tableSchema.getName();

        log.info("delete  solr config " + schema + " start!");

        try {
            if (SolrServerManager.getInstance().existConfig(schema))
                SolrServerManager.getInstance().deleteConfig(schema);
        } catch (IOException e) {
            log.error(e);
            throw new RuntimeException("delete config " + schema + " failure!", e);
        }

//        String path = ConfigUtil.getSolrConfigPath(schema);
//
//        File dir = new File(path);
//
//        boolean isSuccess = true;
//        if (dir.exists()) {
//            isSuccess = deleteDir(dir);
//        }
        log.info("delete  solr config " + schema + " success!");

        return true;
    }

    @Override
    public boolean canExecute() {

        try {
            return !SolrServerManager.getInstance().existConfig(tableSchema.getName());
        } catch (IOException e) {
            throw new RuntimeException("check config " + tableSchema.getName() + " failure!", e);
        }
    }

    private boolean deleteDir(File dir) {
        if (!dir.exists())
            return true;
        if (dir.isDirectory()) {
            String[] children = dir.list();
            if (children == null)
                return true;
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


    private List<Element> generateSolrFields(Schema tableSchema) {

        List<Element> solrFields = new ArrayList<Element>();

        Map<String, Field> fields = tableSchema.getFields();

        fields.values().stream().filter(field -> field.withSolr()).forEach(field -> {
            solrFields.add(asSolrField(field));
        });

        for (ContentField contentField : tableSchema.getContentFields()) {
            solrFields.add(generateContentField(contentField));
            fields.values().stream().filter(f -> StringUtils.equals(contentField.getName(), f.getContentField())).forEach(field -> {

                Element fieldEle = new DefaultElement("copyField");
                fieldEle.add(new DefaultAttribute("source", field.getName()));
                fieldEle.add(new DefaultAttribute("dest", contentField.getName()));
                solrFields.add(fieldEle);
            });
        }

        return solrFields;
    }

    /**
     * generate a solr field
     *
     * @return
     */

    private Element asSolrField(Field field) {

        Element fieldEle = new DefaultElement("field");
        fieldEle.add(new DefaultAttribute("name", field.getName()));
        fieldEle.add(new DefaultAttribute("indexed", String.valueOf(field.isIndexed())));
        fieldEle.add(new DefaultAttribute("stored", String.valueOf(field.isIndexStored())));

        if (field.getStoreType() == FieldType.NETSTED) {
//            field.add(new DefaultAttribute("multiValued",String.valueOf(true)));
        } else if (field.getStoreType() == FieldType.ATTACHMENT) {
            fieldEle.add(new DefaultAttribute("multiValued", String.valueOf(true)));
            fieldEle.add(new DefaultAttribute("type", "string"));
        } else {
            fieldEle.add(new DefaultAttribute("type", field.getIndexType()));
        }
        return fieldEle;
    }


    private Element generateContentField(ContentField contentField) {

        Element fieldEle = new DefaultElement("field");
        fieldEle.add(new DefaultAttribute("name", contentField.getName()));
        fieldEle.add(new DefaultAttribute("indexed", String.valueOf(true)));
        fieldEle.add(new DefaultAttribute("stored", String.valueOf(false)));
        fieldEle.add(new DefaultAttribute("type", contentField.getType()));
        fieldEle.add(new DefaultAttribute("multiValued", String.valueOf(true)));
        return fieldEle;
    }
}
