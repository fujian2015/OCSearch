package com.asiainfo.ocsearch.transaction.atomic;

import com.asiainfo.ocsearch.meta.ContentField;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.apache.commons.io.IOUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultElement;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mac on 2017/3/27.
 */
public class GenerateSolrConfig implements AtomicOperation, Serializable {

    Schema tableSchema;

    public GenerateSolrConfig(Schema tableSchema) {

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

            List<Element> fields = generateSolrFields(tableSchema);

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

        String path = ConfigUtil.getSolrConfigPath(tableSchema.name);

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


    private List<Element> generateSolrFields(Schema tableSchema) {

        List<Element> solrFields = new ArrayList<Element>();

        Map<String, Field> fields = tableSchema.getFields();

        fields.values().stream().filter(field -> field.isIndexStored() || field.isIndexed()||field.isIndexContented()).forEach(field -> {
            solrFields.add(asSolrField(field));
        });

        ContentField contentField = tableSchema.getContentField();
        if (contentField != null) {

            solrFields.add(generateContentField(contentField));

            fields.values().stream().filter(Field::isIndexContented).forEach(field -> {

                Element fieldEle = new DefaultElement("copyField");
                fieldEle.add(new DefaultAttribute("src", field.getName()));
                fieldEle.add(new DefaultAttribute("dst", contentField.getName()));
                solrFields.add(fieldEle);
            });
        }
        return solrFields;
    }


//    @Deprecated
//    public Set<String> getSolrFieldNames() {
//
//        Set<String> solrFields = fields.values().stream()
//                .filter(field -> asSolrField(field) != null)
//                .map(field -> field.name)
//                .collect(Collectors.toSet());
//
//        if (contentField != null) {
//            solrFields.add(contentField.name);
//        }
//        return solrFields;
//    }

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

        return fieldEle;
    }
}
