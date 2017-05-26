package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.indexer.IndexerService;
import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.InnerField;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.asiainfo.ocsearch.utils.JsonWirterUtil;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.XMLWriter;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by mac on 2017/4/6.
 */
public class CreateIndexerTable implements AtomicOperation {

    static Logger log = Logger.getLogger("state");

    final String table;

    final Schema tableSchema;

    public CreateIndexerTable(String table, Schema tableSchema) {

        this.table = table;
        this.tableSchema = tableSchema;
    }

    @Override
    public boolean execute() {

        log.info("create indexer table " + table + " start!");
        try {
            IndexerServiceManager.getIndexerService().createTable(table,getIndexerConf(table));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("create habse-indexer table " + table + " failure!", e);
        } finally {

        }

        log.info("create indexer table " + table + " success!");

        return true;
    }

    @Override
    public boolean recovery() {
        log.info("delete indexer table " + table + " start!");
        try {

            IndexerService indexerService = IndexerServiceManager.getIndexerService();

//            String path = ConfigUtil.getIndexerConfigPath(tableSchema.getName());
//
//            File config = new File(path);
//
//            if (config.exists()) FileUtils.deleteDirectory(config);

            if (indexerService.exists(table)) indexerService.deleteTable(table);

        } catch (Exception e) {
            log.error("delete indexer table " + table + " failure!", e);
            throw new RuntimeException("delete indexer table " + table + " failure!", e);
        }
        log.info("delete indexer table " + table + " success!");
        return true;
    }

    @Override
    public boolean canExecute() {
        return !IndexerServiceManager.getIndexerService().exists(table);
    }


    /**
     * @param conf
     */
    @Deprecated
    private void generateSchema(File conf) {
        ObjectMapper objectMapper = new ObjectMapper();
        FileWriter fileWriter = null;
        try {

            ArrayNode morphlines = objectMapper.createArrayNode();

            ObjectNode morphline = objectMapper.createObjectNode();

            morphlines.add(morphline);

            morphline.put("id", "morphline");

            morphline.put("importCommands", getImportCommands());

            ArrayNode commands = new ObjectMapper().createArrayNode();

            morphline.put("commands", commands);

            commands.add(getExtractCommands());

            commands.add(getLogCommads());

            fileWriter = new FileWriter(conf);

            fileWriter.write("morphlines\t:" + JsonWirterUtil.toConfigString(morphlines, 0));

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("generate indexer schema failure", e);
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {

                }
            }
        }
    }

    /**
     * {
     * "inputColumn": "info:firstname",
     * "outputField": "firstname",
     * "type": "string",
     * "source": "value"
     * },
     *
     * @return
     */
    @Deprecated
    public ArrayNode getIndexerFields() {

        JsonNodeFactory factory = JsonNodeFactory.instance;
        ArrayNode indexerFields = factory.arrayNode();
        Set<String> innerNames = new HashSet<>();

        tableSchema.getFields().values().stream().filter(Field::withSolr).forEach(field -> {
            if (StringUtils.isNotEmpty(field.getInnerField())) {
                innerNames.add(field.getInnerField());
            } else {
                ObjectNode fieldNode = factory.objectNode();

                fieldNode.put("inputColumn", StringUtils.join(field.getHbaseFamily(), ":", field.getHbaseColumn()));

                fieldNode.put("outputField", field.getName());

                FieldType fieldType = field.getStoreType();

                if (fieldType == FieldType.NETSTED) {
                    fieldNode.put("type", "com.ngdata.hbaseindexer.parse.JsonByteArrayValueMapper");

                } else {
                    fieldNode.put("type", fieldType.getValue());
                }

                fieldNode.put("source", "value");

                indexerFields.add(fieldNode);
            }
        });

        Map<String, InnerField> innerFieldMap = tableSchema.getInnerFields();

        innerNames.forEach(innerName -> {
            InnerField field = innerFieldMap.get(innerName);
            ObjectNode fieldNode = factory.objectNode();

            fieldNode.put("inputColumn", StringUtils.join(field.getHbaseFamily(), ":", field.getHbaseColumn()));

            fieldNode.put("outputField", field.getName());

            fieldNode.put("type", "com.ngdata.hbaseindexer.parse.InnerValueMapper");

            fieldNode.put("source", "value");

            indexerFields.add(fieldNode);
        });

        return indexerFields;
    }
@Deprecated
    private ObjectNode getExtractCommands() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode extract = objectMapper.createObjectNode();

        ObjectNode extractHbaseCells = objectMapper.createObjectNode();

        extract.put("extractHBaseCells", extractHbaseCells);

        extractHbaseCells.put("mappings", getIndexerFields());

        return extract;
    }

@Deprecated
    private ObjectNode getLogCommads() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode logCommands = objectMapper.createObjectNode();

        logCommands.put("format", "output record: {}");

        ArrayNode args = objectMapper.createArrayNode();

        args.add("@{}");
        logCommands.put("args", args);

        return logCommands;
    }
@Deprecated
    private ArrayNode getImportCommands() {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode importCommands = objectMapper.createArrayNode();

        importCommands.add("org.kitesdk.morphline.**");

        importCommands.add("com.ngdata.**");

        return importCommands;
    }

    private byte[] getIndexerConf(String name) throws IOException {

        Document indexerDoc = DocumentHelper.createDocument();

        Element indexer = indexerDoc.addElement("indexer");
        indexer.addAttribute("table", name);
        indexer.addAttribute("mapper", "com.ngdata.hbaseindexer.parse.DefaultResultToSolrMapper");
        indexer.addAttribute("table-name-field", "_table_");
        indexer.addAttribute("read-row", "never");

        Element isProduct = indexer.addElement("param");
        isProduct.addAttribute("name", "isProductionMode");
        isProduct.addAttribute("value", "true");

        Map<String, Element> innerElements = Maps.newHashMap();

        tableSchema.getFields().values().stream().filter(Field::withSolr).forEach(field -> {
            if (StringUtils.isNotEmpty(field.getInnerField())) {
                String innerName = field.getInnerField();
                if (false==innerElements.containsKey(innerName)) {

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
            System.out.println(indexerDoc.asXML());
        } catch (IOException e) {
            throw e;
        } finally {
            if (xmlWriter != null)
                xmlWriter.close();
        }

        return out.toByteArray();
    }
}
