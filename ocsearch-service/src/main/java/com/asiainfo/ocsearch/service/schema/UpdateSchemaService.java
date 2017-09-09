package com.asiainfo.ocsearch.service.schema;

import com.asiainfo.ocsearch.datasource.solr.SolrServer;
import com.asiainfo.ocsearch.datasource.solr.SolrServerManager;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.meta.Field;
import com.asiainfo.ocsearch.meta.FieldType;
import com.asiainfo.ocsearch.meta.Schema;
import com.asiainfo.ocsearch.metahelper.MetaDataHelper;
import com.asiainfo.ocsearch.metahelper.MetaDataHelperManager;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.codehaus.jackson.JsonNode;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by mac on 2017/4/28.
 */
public class UpdateSchemaService extends OCSearchService {

    static Logger log = Logger.getLogger("state");

    @Override
    public byte[] doService(JsonNode request) throws ServiceException {

        try {
            String command = request.get("command").asText();

            String table = request.get("table").asText();

            MetaDataHelper metaDataHelper = MetaDataHelperManager.getInstance();

            Schema schema = metaDataHelper.getSchemaByTable(table);

            if (schema == null)
                throw new ServiceException("the  'table' " + table + " does not exist", ErrorCode.TABLE_NOT_EXIST);

            String lock = metaDataHelper.lock("SCHEMA_" + schema.name);

            if (lock == null)
                throw new ServiceException("get lock failure,please check lock file on zookeeper", ErrorCode.TABLE_EXIST);

            try {
                JsonNode body = request.get("field");

                if (StringUtils.equals(command, "update_field")) {
                    updateField(body, schema, table);
                } else if (StringUtils.equals(command, "add_field")) {
                    addField(body, schema, table);
                } else if (StringUtils.equals(command, "delete_field")) {
                    deleteField(body, schema, table);
                } else {
                    throw new ServiceException("unknown update command " + command, ErrorCode.PARSE_ERROR);
                }
            }finally {
                metaDataHelper.unlock(lock);
            }
            return success;
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }

    private void deleteField(JsonNode body, Schema schema, String table) throws ServiceException {
        try {

            Schema schemaNew = (Schema) schema.clone();

            String name = body.get("name").asText();

            if (!schema.getFields().containsKey(name))
                throw new ServiceException("the field '" + name + "' does not exist!", ErrorCode.PARSE_ERROR);

            Field field = schema.getFields().get(name);

            if (StringUtils.isNotEmpty(field.getInnerField())) {
                throw new ServiceException("doesn't support delete a inner field", ErrorCode.PARSE_ERROR);
            }
            if (field.withSolr()) {

                Map<String, Object> params = new HashMap<>();
                params.put("name", name);
                params.put("indexed", String.valueOf(field.isIndexed()));
                params.put("stored", String.valueOf(field.isIndexStored()));
                params.put("type", field.getIndexType());

                SolrServer solrServer = SolrServerManager.getInstance();

                List<SchemaRequest.Update> updates = new ArrayList<>(2);

                updates.add(new SchemaRequest.DeleteField(name));
                if (StringUtils.isNotEmpty(field.getContentField())) {
                    updates.add(new SchemaRequest.DeleteCopyField(name, Arrays.asList(field.getContentField())));
                }

                log.info("update field '" + name + "' in solr config start!");
                solrServer.updateFields(updates, table);
                log.info("update field '" + name + "' in solr config success!");
            }
            log.info("update field '" + name + "' in zookeeper  start!");
            schemaNew.getFields().remove(name);
            MetaDataHelperManager.getInstance().updateSchema(schemaNew);
            log.info("update field '" + name + "' in zookeeper  success!");
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }

    private void addField(JsonNode body, Schema schema, String table) throws ServiceException {
        try {

            Schema schemaNew = (Schema) schema.clone();

            String name = body.get("name").asText();

            if (schema.getFields().containsKey(name))
                throw new ServiceException("the field '" + name + "' exists!", ErrorCode.PARSE_ERROR);

            Field field = new Field(body);

            if (StringUtils.isNotEmpty(field.getInnerField())) {
                throw new ServiceException("doesn't support add a inner field", ErrorCode.PARSE_ERROR);
            }
            if(StringUtils.isEmpty(field.getHbaseFamily())||StringUtils.isEmpty(field.getHbaseColumn())){
                throw new ServiceException("doesn't support add a field without hbase family or column ", ErrorCode.PARSE_ERROR);
            }
            if (field.withSolr()) {
                if (field.isIndexContented() && !schemaContainsCf(schema, field.getContentField())) {
                    throw new ServiceException("the schema does not has the content field " + field.getContentField(), ErrorCode.PARSE_ERROR);
                }
                Map<String, Object> params = new HashMap<>();
                params.put("name", name);
                params.put("indexed", String.valueOf(field.isIndexed()));
                params.put("stored", String.valueOf(field.isIndexStored()));
                params.put("type", field.getIndexType());

                SolrServer solrServer = SolrServerManager.getInstance();

                List<SchemaRequest.Update> updates = new ArrayList<>(2);

                updates.add(new SchemaRequest.AddField(params));
                if (StringUtils.isNotEmpty(field.getContentField())) {
                    updates.add(new SchemaRequest.AddCopyField(name, Arrays.asList(field.getContentField())));
                }

                log.info("update field '" + name + "' in solr config start!");
                solrServer.updateFields(updates, table);
                log.info("update field '" + name + "' in solr config success!");
            }
            log.info("update field '" + name + "' in zookeeper  start!");
            schemaNew.getFields().put(name, field);
            MetaDataHelperManager.getInstance().updateSchema(schemaNew);
            log.info("update field '" + name + "' in zookeeper  success!");
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }

    private void updateField(JsonNode body, Schema schema, String table) throws ServiceException {
        try {

            Schema schemaNew = (Schema) schema.clone();

            String name = body.get("name").asText();

            Field oriField = schema.getFields().get(name);

            Field field = schemaNew.getFields().get(name);

            constructField(field, body);


            if (oriField.equals(field)) {
                throw new ServiceException("the field equals the origin field", ErrorCode.PARSE_ERROR);
            } else if (!oriField.indexEquals(field)) {

                if (field.isIndexContented() && !schemaContainsCf(schema, field.getContentField())) {
                    throw new ServiceException("the schema does not has the content field " + field.getContentField(), ErrorCode.PARSE_ERROR);
                }
                Map<String, Object> params = new HashMap<>();
                params.put("name", name);
                params.put("indexed", String.valueOf(field.isIndexed()));
                params.put("stored", String.valueOf(field.isIndexStored()));
                params.put("type", field.getIndexType());

                SolrServer solrServer = SolrServerManager.getInstance();

                List<SchemaRequest.Update> updates = new ArrayList<>(2);

                if (oriField.withSolr() && field.withSolr()) {

                    updates.add(new SchemaRequest.ReplaceField(params));

                    if (StringUtils.equals(oriField.getContentField(), field.getContentField())) {

                    } else if (StringUtils.isEmpty(oriField.getContentField())) {
                        updates.add(new SchemaRequest.AddCopyField(name, Arrays.asList(field.getContentField())));
                    } else if (StringUtils.isEmpty(field.getContentField())) {
                        updates.add(new SchemaRequest.DeleteCopyField(name, Arrays.asList(oriField.getContentField())));
                    } else {
                        updates.add(new SchemaRequest.DeleteCopyField(name, Arrays.asList(oriField.getContentField())));
                        updates.add(new SchemaRequest.AddCopyField(name, Arrays.asList(field.getContentField())));
                    }
                } else if (oriField.withSolr()) {
                    updates.add(new SchemaRequest.DeleteField(name));
                    if (StringUtils.isNotEmpty(oriField.getContentField())) {
                        updates.add(new SchemaRequest.DeleteCopyField(name, Arrays.asList(oriField.getContentField())));
                    }
                } else if (field.withSolr()) {
                    updates.add(new SchemaRequest.AddField(params));
                    if (StringUtils.isNotEmpty(field.getContentField())) {
                        updates.add(new SchemaRequest.AddCopyField(name, Arrays.asList(field.getContentField())));
                    }
                }
                log.info("update field '" + name + "' in solr config start!");
                solrServer.updateFields(updates, table);
                log.info("update field '" + name + "' in solr config success!");
            }
            log.info("update field '" + name + "' in zookeeper  start!");
            MetaDataHelperManager.getInstance().updateSchema(schemaNew);
            log.info("update field '" + name + "' in zookeeper  success!");
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }

    private void constructField(Field field, JsonNode body) throws ServiceException {
        try {
            if (body.has("content_field")) {
                String contentField = body.get("content_field").asText();
                if (StringUtils.isBlank(contentField))
                    field.setContentField(null);
                else
                    field.setContentField(contentField);
            }
            if (body.has("indexed"))
                field.setIndexed(body.get("indexed").asBoolean());
            if (body.has("index_stored"))
                field.setIndexStored(body.get("index_stored").asBoolean());
            if (body.has("index_type")) {
                String indexType = body.get("index_type").asText();
                if (StringUtils.isBlank(indexType))
                    field.setIndexType(null);
                else
                    field.setIndexType(indexType);
            }
            if (body.has("store_type"))
                field.setStoreType(FieldType.valueOf(body.get("store_type").asText().toUpperCase()));
            if (body.has("hbase_column"))
                field.setHbaseColumn(body.get("hbase_column").asText());
            if (body.has("hbase_family"))
                field.setHbaseFamily(body.get("hbase_family").asText());
            if (body.has("inner_field") || body.has("inner_index"))
                throw new ServiceException("the inner_field and inner_index can't be updated!", ErrorCode.PARSE_ERROR);
        } catch (ServiceException se) {
            throw se;
        } catch (Exception e) {
            throw new ServiceException(e, ErrorCode.PARSE_ERROR);
        }
    }

    private boolean schemaContainsCf(Schema schema, String name) {
        Set<String> contentFields = schema.getContentFields().stream().map(cf -> cf.getName()).collect(Collectors.toSet());
        return contentFields.contains(name);
    }

}
