package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.datasource.indexer.IndexerService;
import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.InnerField;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.atomic.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import com.asiainfo.ocsearch.utils.JsonWirterUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

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

        String schema = tableSchema.getName();

        log.info("create indexer table " + table + " start!");

        String path = ConfigUtil.getIndexerConfigPath(schema);

        File config = new File(path);
        try {

            if (config.exists()) FileUtils.deleteDirectory(config);

            config.mkdirs();
            File conf = new File(path, "morphlines.conf");
            generateSchema(conf);
            IndexerServiceManager.getIndexerService().createTable(table, new File(path, "morphlines.conf").getAbsolutePath());

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("create habse-indexer table " + table + " failure!", e);
        } finally {
            try {
                if (config.exists()) FileUtils.deleteDirectory(config);
            } catch (IOException e) {
                log.error("delete indexer dir error!", e);
                throw new RuntimeException("delete indexer dir error!", e);
            }
        }

        log.info("create indexer table " + table + " success!");

        return true;
    }


    @Override
    public boolean recovery() {
        log.info("delete indexer table " + table + " start!");
        try {

            IndexerService indexerService2 = IndexerServiceManager.getIndexerService();

            String path = ConfigUtil.getIndexerConfigPath(tableSchema.getName());

            File config = new File(path);

            if (config.exists()) FileUtils.deleteDirectory(config);

            if (indexerService2.exists(table)) indexerService2.deleteTable(table);

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

        Map<String,InnerField> innerFieldMap=tableSchema.getInnerFields();

        innerNames.forEach(innerName->{
            InnerField field=innerFieldMap.get(innerName);
            ObjectNode fieldNode = factory.objectNode();

            fieldNode.put("inputColumn", StringUtils.join(field.getHbaseFamily(), ":", field.getHbaseColumn()));

            fieldNode.put("outputField", field.getName());

            fieldNode.put("type", "com.ngdata.hbaseindexer.parse.InnerValueMapper");

            fieldNode.put("source", "value");

            indexerFields.add(fieldNode);
        });

        return indexerFields;
    }

    private ObjectNode getExtractCommands() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode extract = objectMapper.createObjectNode();

        ObjectNode extractHbaseCells = objectMapper.createObjectNode();

        extract.put("extractHBaseCells", extractHbaseCells);

        extractHbaseCells.put("mappings", getIndexerFields());

        return extract;
    }


    private ObjectNode getLogCommads() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode logCommands = objectMapper.createObjectNode();

        logCommands.put("format", "output record: {}");

        ArrayNode args = objectMapper.createArrayNode();

        args.add("@{}");
        logCommands.put("args", args);

        return logCommands;
    }

    private ArrayNode getImportCommands() {
        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode importCommands = objectMapper.createArrayNode();

        importCommands.add("org.kitesdk.morphline.**");

        importCommands.add("com.ngdata.**");

        return importCommands;
    }
}
