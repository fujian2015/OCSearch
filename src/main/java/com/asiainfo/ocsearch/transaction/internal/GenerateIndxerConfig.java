package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.core.TableSchema;
import com.asiainfo.ocsearch.transaction.AtomicOperation;
import com.asiainfo.ocsearch.utils.ConfigUtil;
import com.asiainfo.ocsearch.utils.JsonWirterUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by mac on 2017/4/5.
 */
public class GenerateIndxerConfig implements AtomicOperation {


    TableSchema tableSchema;

    public GenerateIndxerConfig(TableSchema tableSchema) {

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

    ObjectMapper objectMapper = new ObjectMapper();

    /**
     * @param conf
     */
    private void generateSchema(File conf) {
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
            throw new RuntimeException("generate solr schema failure", e);
        } finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {

                }
            }
        }
    }

    private ObjectNode getExtractCommands() {
        ObjectNode extract = objectMapper.createObjectNode();

        ObjectNode extractHbaseCells = objectMapper.createObjectNode();

        extract.put("extractHBaseCells", extractHbaseCells);

        extractHbaseCells.put("mappings", tableSchema.getIndexerFields());

        return extract;
    }


    private ObjectNode getLogCommads() {

        ObjectNode logCommands = objectMapper.createObjectNode();

        logCommands.put("format","output record: {}");

        ArrayNode args=objectMapper.createArrayNode();

        args.add("@{}");
        logCommands.put("args",args);

        return logCommands;
    }

    private ArrayNode getImportCommands() {

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
