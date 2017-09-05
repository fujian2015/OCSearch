package com.asiainfo.ocsearch.meta;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mac on 2017/3/24.
 */
public class Schema implements Serializable, Cloneable {

    static final String BASIC_FAMILY = "B";
    static final String FILE_FAMILY = "C";
    static final String ATTACHMENT_FAMILY = "D";
    static final String NETSTED_FAMILY = "E";

    public final String name;

    boolean withHbase = false;

    String idFormatter = null;

    IndexType indexType;  //-1: hbase ,0 solr+hbase hbase-indexer


    private final String tableExpression;

    private final String rowkeyExpression;   //0 自动生成id  1 指定生成规则

    private Map<String, Field> fields = new HashMap<String, Field>();

    private List<ContentField> contentFields = new ArrayList<>();  //default query field
    private List<QueryField> queryFields = new ArrayList<QueryField>();

    private Map<String, InnerField> innerFields = new HashMap<>();

    private Multimap<String, Field> innerMap = ArrayListMultimap.create();

    public Schema(String name, String tableExpression, String rowkeyExpression) {
        this.name = name;
        this.tableExpression = tableExpression;
        this.rowkeyExpression = rowkeyExpression;
    }


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
            boolean isRequest = request.has("request") ? request.get("request").asBoolean() : false;

            if (request.has("with_hbase") && request.get("with_hbase").asBoolean())
                withHbase = true;

            this.name = request.get("name").getTextValue();

            this.indexType = IndexType.valueOf(request.get("index_type").asInt());

            this.rowkeyExpression = request.get("rowkey_expression").asText();
            this.tableExpression = request.get("table_expression").asText();

            if (request.has("id_formatter"))
                idFormatter = request.get("id_formatter").asText();

            ArrayNode contentNodes = (ArrayNode) request.get("content_fields");
            contentNodes.forEach(contentNode -> contentFields.add(new ContentField(contentNode)));

            ArrayNode fieldsNode = (ArrayNode) request.get("fields");

            for (JsonNode fieldNode : fieldsNode) {

                Field field = new Field(fieldNode);
                fields.put(field.name, field);
            }

            ArrayNode queryFieldsNode = (ArrayNode) request.get("query_fields");

            queryFieldsNode.forEach(queryFieldNode -> this.queryFields.add(new QueryField(queryFieldNode)));


            ArrayNode innerNodes = (ArrayNode) request.get("inner_fields");

            innerNodes.forEach(innerNode -> {
                InnerField innerField = new InnerField(innerNode);
                innerFields.put(innerField.getName(), innerField);
            });


            if (isRequest) {
                String checkResult = checkFields();
                if (null != checkResult)
                    throw new ServiceException(checkResult, ErrorCode.PARSE_ERROR);
                fillBlanks();
            } else {
                //initial inner map
                for (Field field : this.fields.values()) {

                    String innerField = field.getInnerField();

                    if (innerField != null) {

                        this.innerMap.put(innerField, field);
                    }
                }
            }


        } catch (ServiceException se) {
            throw se;

        } catch (Exception e) {
            e.printStackTrace();
            throw new ServiceException("request is not valid", ErrorCode.PARSE_ERROR);
        }
    }

    private void fillBlanks() throws ServiceException {
        //use the origin name when using phoenix sql
        if (indexType == IndexType.HBASE_PHOENIX || indexType == IndexType.HBASE_SOLR_PHOENIX) {
            for (Field field : this.fields.values()) {
                field.setHbaseFamily(BASIC_FAMILY);
                field.setHbaseColumn(field.name);
            }
            return;
        }

        List<Field> basicFields = new ArrayList<>();
        List<Field> fileFields = new ArrayList<>();
        List<Field> attachmentFields = new ArrayList<>();

        for (Field field : this.fields.values()) {

            String innerField = field.getInnerField();

            if (innerField == null) {
                if (field.getStoreType() == FieldType.ATTACHMENT)
                    attachmentFields.add(field);

                else if (field.getStoreType() == FieldType.FILE)
                    fileFields.add(field);

                else
                    basicFields.add(field);
            } else {
                this.innerMap.put(innerField, field);
            }
        }

        if (innerMap.keySet().size() == this.innerFields.size()) {
            for (InnerField innerField : this.innerFields.values()) {
                int order = 0;
                if (!innerMap.containsKey(innerField.name))
                    throw new ServiceException("inner field " + innerField.getName() + " never be used in field list ", ErrorCode.PARSE_ERROR);
                for (Field f : innerMap.get(innerField.name)) {
                    f.setInnerIndex(order++);
                }
            }
        } else {
            throw new ServiceException("inner field  must be in use properly! ", ErrorCode.PARSE_ERROR);
        }


        //basic hbase family
        int basicOrder = 0;
        for (Field field : basicFields) {
            field.setHbaseFamily(BASIC_FAMILY);
            field.setHbaseColumn(String.valueOf(basicOrder++));
        }
        for (InnerField field : this.innerFields.values()) {
            field.setHbaseFamily(BASIC_FAMILY);
            field.setHbaseColumn(String.valueOf(basicOrder++));
        }

        //file type family
        int fileOrder = 0;
        for (Field field : fileFields) {
            field.setHbaseFamily(FILE_FAMILY);
            field.setHbaseColumn(field.name);
        }

        //attachment type family
        int attachmentOrder = 0;
        if (attachmentFields.size() == 1) {
            Field field = attachmentFields.get(0);
            field.setHbaseFamily(ATTACHMENT_FAMILY);
            field.setHbaseColumn(Constants.FILE_NAMES_COLUMN);
        } else {
            for (Field field : attachmentFields) {
                field.setHbaseFamily(ATTACHMENT_FAMILY + (attachmentOrder++));
                field.setHbaseColumn(Constants.FILE_NAMES_COLUMN);  //存放文件名列表
            }
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
            boolean hasQf = false;
            for (ContentField contentField : contentFields) {
                if (contentField.getName().equals(qf.name)) {
                    hasQf = true;
                }
            }
            if (hasQf) continue;
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

        if (idFormatter != null)
            schemaNode.put("id_formatter", idFormatter);

        if (withHbase == true)
            schemaNode.put("with_hbase", withHbase);

        ArrayNode contentNodes = factory.arrayNode();
        contentFields.forEach(contentField -> contentNodes.add(contentField.toJsonNode()));
        schemaNode.put("content_fields", contentNodes);

        ArrayNode innerNodes = factory.arrayNode();
        innerFields.values().forEach(innerField -> innerNodes.add(innerField.toJsonNode()));
        schemaNode.put("inner_fields", innerNodes);

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


    @Override
    public Object clone() {

        try {
            return new Schema(this.toJsonNode());
        } catch (ServiceException e) {
            e.printStackTrace();
        }
        return new Object();
    }


    public IndexType getIndexType() {
        return indexType;
    }

    public List<ContentField> getContentFields() {
        return contentFields;
    }

    public void setContentFields(List<ContentField> contentFields) {
        this.contentFields = contentFields;
    }

    public Map<String, InnerField> getInnerFields() {
        return innerFields;
    }

    public void setInnerFields(Map<String, InnerField> innerFields) {
        this.innerFields = innerFields;
    }

    public Multimap<String, Field> getInnerMap() {
        return innerMap;
    }


    public String getIdFormatter() {
        return idFormatter;
    }

    public boolean withHbase() {
        return  withHbase;
    }
}
