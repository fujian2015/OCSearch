package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.constants.OCSearchEnv;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.InnerField;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by mac on 2017/7/11.
 */
public abstract class UpdateOrAddIndexer implements AtomicOperation {
    final String table;

    final Schema tableSchema;

    public UpdateOrAddIndexer(String table, Schema tableSchema) {

        this.table = table;
        this.tableSchema = tableSchema;
    }

    protected byte[] getIndexerConf(String name) throws IOException {

        Document indexerDoc = DocumentHelper.createDocument();

        Element indexer = indexerDoc.addElement("indexer");
        indexer.addAttribute("table", name);
        indexer.addAttribute("mapper", "com.ngdata.hbaseindexer.parse.DefaultResultToSolrMapper");

        if (tableSchema.getIdFormatter() != null)
            indexer.addAttribute("unique-key-formatter", tableSchema.getIdFormatter());

        indexer.addAttribute("table-name-field", "_table_");
        indexer.addAttribute("read-row", "never");

        Element isProduct = indexer.addElement("param");
        isProduct.addAttribute("name", "isProductionMode");
        isProduct.addAttribute("value", "true");


        if (Boolean.valueOf(OCSearchEnv.getEnvValue("indexer.tablecf.set", "true")) == true) {
            Element setTableCF = indexer.addElement("param");
            setTableCF.addAttribute("name", "set-tablef");
            setTableCF.addAttribute("value", "true");
        }

        Map<String, Element> innerElements = Maps.newHashMap();

        tableSchema.getFields().values().stream().filter(Field::withSolr).forEach(field -> {
            if (StringUtils.isNotEmpty(field.getInnerField())) {
                String innerName = field.getInnerField();
                if (false == innerElements.containsKey(innerName)) {

                    InnerField inf = tableSchema.getInnerFields().get(innerName);
                    Element innerElement = indexer.addElement("field");
                    innerElement.addAttribute("name", inf.getName());
                    innerElement.addAttribute("source", "value");
                    innerElement.addAttribute("value", inf.getHbaseFamily() + ":" + inf.getHbaseColumn());
                    innerElement.addAttribute("type", "com.ngdata.hbaseindexer.parse.InnerFieldArrayValueMapper");

                    Element split = innerElement.addElement("param");
                    split.addAttribute("name", "_split_");
                    split.addAttribute("value", inf.getSeparator());
                    innerElements.put(innerName, innerElement);
                }
                Element f = innerElements.get(innerName).addElement("param");
                f.addAttribute("name", field.getName());
                f.addAttribute("value", field.getInnerIndex() + ":" + field.getStoreType().toString().toLowerCase());
            } else {
                Element fieldElement = indexer.addElement("field");
                fieldElement.addAttribute("name", field.getName());
                fieldElement.addAttribute("source", "value");
                fieldElement.addAttribute("value", field.getHbaseFamily() + ":" + field.getHbaseColumn());
                fieldElement.addAttribute("type", field.getStoreType().toString().toLowerCase());
            }
        });

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
}
