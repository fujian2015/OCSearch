package com.asiainfo.ocsearch.meta;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by mac on 2017/3/24.
 */
public class Schema implements Serializable {


    public final String name;

    IndexType indexType;  //-1: hbase ,0 solr+hbase hbase-indexer

    public ContentField contentField = null;  //default query field

    private final String tableExpression;

    private final String rowkeyExpression;   //0 自动生成id  1 指定生成规则

    private Map<String, Field> fields = new HashMap<String, Field>();

    private List<QueryField> queryFields = new ArrayList<QueryField>();


    /**
     * name	varchar(255)	NO	PRI	NULL	schema名
     * rowkey_expression	varchar(255)	NO		NULL	计算rowkey的表达式（ocsearch支持的语法）
     * table_expression	varchar(255)	NO		NULL	计算表名的表达式（ocsearch支持的语法）
     * content_field	varchar(255)	NO		NULL	默认查询字段{"key":"tests","type":""}
     * query_fields	varchar(255)	NO		NULL	查询字段列表[{"key":"title","weight":20}]
     *
     * @param request
     * @throws ServiceException
     */
    public Schema(JsonNode request) throws ServiceException {

        try {

            this.name = request.get("name").getTextValue();

            this.indexType=IndexType.valueOf(request.get("index_type").asInt());

            this.rowkeyExpression = request.get("rowkey_expression").asText();
            this.tableExpression = request.get("table_expression").asText();

            if (request.get("content_field") != null)
                this.contentField = new ContentField(request.get("content_field"));

            ArrayNode fieldsNode = (ArrayNode) request.get("fields");

            for (JsonNode fieldNode : fieldsNode) {

                Field field = new Field(fieldNode);
                fields.put(field.name, field);
            }

            ArrayNode queryFieldsNode = (ArrayNode) request.get("query_fields");

            if (queryFieldsNode != null) {
                for (JsonNode queryFieldNode : queryFieldsNode) {

                    this.queryFields.add(new QueryField(queryFieldNode));
                }
            }

            String checkResult = checkFields();
            if (null != checkResult)
                throw new ServiceException(checkResult, ErrorCode.PARSE_ERROR);

        } catch (ServiceException se) {
            throw se;

        } catch (Exception e) {
            e.printStackTrace();
            throw new ServiceException("request is not valid", ErrorCode.PARSE_ERROR);
        }
    }

    public Schema(String name, String tableExpression, String rowkeyExpression, IndexType indexType) {


        this.name = name;
        this.rowkeyExpression = rowkeyExpression;
        this.tableExpression = tableExpression;
        this.indexType = indexType;

    }

    private String checkFields() {

        for (QueryField qf : queryFields) {
            if (fields.containsKey(qf.name))
                continue;

            if (contentField != null && qf.name.equals(contentField.name))
                continue;
            return "the query field  " + qf.name + "is not in the fields";
        }
        return null;
    }


    public List<QueryField> getQueryFields() {
        return queryFields;
    }

    @Override
    public String toString() {
        return toJsonNode().toString();

    }

    public JsonNode toJsonNode() {

        JsonNodeFactory factory = JsonNodeFactory.instance;

        ObjectNode schemaNode = factory.objectNode();
        schemaNode.put("name", name);
        schemaNode.put("rowkey_expression", rowkeyExpression);
        schemaNode.put("table_expression", tableExpression);
        schemaNode.put("index_type", indexType.getValue());

        if (contentField != null)
            schemaNode.put("content_field", contentField.toJsonNode());

        ArrayNode queryNodes = factory.arrayNode();

        for (QueryField qf : queryFields) {
            queryNodes.add(qf.toJsonNode());
        }
        schemaNode.put("query_fields", queryNodes);

        ArrayNode fieldNodes = factory.arrayNode();

        for (Field f : fields.values()) {
            fieldNodes.add(f.toJsonNode());
        }
        schemaNode.put("fields", fieldNodes);

        return schemaNode;
    }

    public String getName() {
        return name;
    }

    public void setContentField(ContentField contentField) {
        this.contentField = contentField;
    }

    public String getTableExpression() {
        return tableExpression;
    }

    public String getRowkeyExpression() {
        return rowkeyExpression;
    }

    public void setFields(Map<String, Field> fields) {
        this.fields = fields;
    }

    public void setQueryFields(List<QueryField> queryFields) {
        this.queryFields = queryFields;
    }

    public Map<String, Field> getFields() {
        return fields;
    }

    public ContentField getContentField() {
        return contentField;
    }

    @Override
    public Object clone() {

        Schema schema = new Schema(this.name, this.tableExpression, this.rowkeyExpression, this.indexType);

        Map<String, Field> fieldsMap = new HashMap<>();
        for (Map.Entry<String, Field> entry : this.fields.entrySet()) {
            fieldsMap.put(entry.getKey(), (Field) entry.getValue().clone());
        }
        schema.setFields(fieldsMap);

        schema.setQueryFields(queryFields.stream().map(qf -> (QueryField) qf.clone()).collect(Collectors.toList()));

        schema.setContentField(contentField == null ? contentField : (ContentField) contentField.clone());

        return schema;
    }


    public IndexType getIndexType() {
        return indexType;
    }
}
