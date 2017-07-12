package com.asiainfo.ocsearch.transaction.atomic.table;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.datasource.indexer.IndexerService;
import com.asiainfo.ocsearch.datasource.indexer.IndexerServiceManager;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.InnerField;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.utils.JsonWirterUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by mac on 2017/4/6.
 */
public class CreateIndexerTable extends UpdateOrAddIndexer {

    static Logger log = Logger.getLogger("state");

    public CreateIndexerTable(String table, Schema tableSchema) {

        super(table,tableSchema);
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
        OutputStreamWriter fileWriter = null;
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

            fileWriter = new OutputStreamWriter(new FileOutputStream(conf), Constants.DEFUAT_CHARSET);

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


}
