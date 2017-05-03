package com.asiainfo.ocsearch.transaction.atomic;

import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import com.asiainfo.ocsearch.utils.JsonWirterUtil;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by mac on 2017/4/5.
 */
public class GenerateIndxerConfig implements AtomicOperation,Serializable {


    Schema tableSchema;

    public GenerateIndxerConfig(Schema tableSchema) {

        this.tableSchema = tableSchema;
    }

    public boolean execute() {

        String path = ConfigUtil.getIndexerConfigPath(tableSchema.name);

        File config = new File(path);

        if (config.exists()) {
            throw new RuntimeException("the work dir exists " + path);
        }

        config.mkdirs();

        File conf = new File(path, "morphlines.conf");


        generateSchema(conf);

        return true;
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

            morphline.put("id", tableSchema.name);

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

        tableSchema.getFields().values().stream().filter(Field::withSolr).forEach(field -> {

            ObjectNode fieldNode = factory.objectNode();

            fieldNode.put("inputColumn", StringUtils.join(field.getHbaseFamily(), ":", field.getHbaseColumn()));

            fieldNode.put("outputField", field.getName());

            FieldType fieldType = field.getStoreType();

            if (fieldType == FieldType.NETSTED) {
                fieldNode.put("type", "com.ngdata.hbaseindexer.parse.JsonByteArrayValueMapper");

            } else if (fieldType == FieldType.ATTACHMENT) {
                fieldNode.put("type", "string");
            } else {
                fieldNode.put("type", fieldType.getValue());
            }

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

    @Override
    public boolean recovery() {

        String path = ConfigUtil.getIndexerConfigPath(tableSchema.name);

        File dir = new File(path);

        if (dir.exists()) {
            return deleteDir(dir);
        }
        return true;
    }

    @Override
    public boolean canExecute() {

        String path = ConfigUtil.getIndexerConfigPath(tableSchema.name);

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
