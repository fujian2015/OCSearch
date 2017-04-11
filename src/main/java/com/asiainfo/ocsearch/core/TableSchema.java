package com.asiainfo.ocsearch.core;

import com.asiainfo.ocsearch.exception.ErrCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.dom4j.Element;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultElement;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by mac on 2017/3/24.
 */
public class TableSchema implements Serializable {

    public final String storeType;
    public final int storePeriod;
    public final String name;

    public final String hbaseTbale;
    public final boolean hbaseExist;
    public final int hbaseRegions;

    public final String solrCollection;
    public final boolean solrExist;
    public final int solrShards;

    public final String partition;  //must has a date format  or a timestamp format

    public final String contentField;  //default query field

    public final FieldType contentType;  //default query field

    public final int rowKeyVersion;   //0 自动生成id  1 指定生成规则

    private Set<KeyField> keyFields = new TreeSet<KeyField>();


    private Map<String, Field> fields = new HashMap<String, Field>();

    private List<QueryField> queryFields = new ArrayList<QueryField>();

    private List<BaseField> baseFields = new ArrayList<BaseField>();


    public TableSchema(JsonNode request) throws ServiceException {

        try {

            this.name = request.get("name").getTextValue();

            //---------------------------------------

            JsonNode storeNode = request.get("store");

            this.storeType = storeNode.get("type").getTextValue();

            if (!StringUtils.equals(this.storeType, "n")) {
                this.storePeriod = storeNode.get("period").getIntValue();
                this.partition = storeNode.get("partition").getTextValue();
            } else {
                this.storePeriod = -1;
                this.partition = "";
            }

            //---------------------------------------

            JsonNode contentNode = request.get("content");
            if (contentNode != null) {
                this.contentField = contentNode.get("name").getTextValue();
                this.contentType = FieldType.valueOf(contentNode.get("type").getTextValue().toUpperCase());
            } else {
                this.contentField = "";
                this.contentType = FieldType.NONE;
            }
            //------------------------------------------

            JsonNode hbaseNode = request.get("hbase");
            if (hbaseNode != null) {
                this.hbaseTbale = hbaseNode.get("name").getTextValue();
                this.hbaseExist = hbaseNode.get("exist").getBooleanValue();
                this.hbaseRegions = hbaseNode.get("regions").getIntValue();
            } else {
                this.hbaseTbale = this.name;
                this.hbaseExist = false;
                this.hbaseRegions = 0;
            }
            //---------------------------------------
            JsonNode solrNode = request.get("solr");

            if (solrNode != null) {

                this.solrCollection = solrNode.get("name").getTextValue();
                this.solrExist = solrNode.get("exist").getBooleanValue();
                this.solrShards = solrNode.get("shards").getIntValue();
            } else {
                this.solrCollection = this.name;
                this.solrExist = false;
                this.solrShards = 0;
            }

            //---------------------------------------

            ArrayNode fieldsNode = (ArrayNode) request.get("fields");

            for (JsonNode fieldNode : fieldsNode) {

                Field field = new Field(fieldNode, this.hbaseExist);
                fields.put(field.name, field);
            }

            //---------------------------------------
            ArrayNode queryFieldsNode = (ArrayNode) request.get("queryfields");

            if (queryFieldsNode != null) {
                for (JsonNode queryFieldNode : queryFieldsNode) {

                    this.queryFields.add(new QueryField(queryFieldNode));
                }
            }
            //---------------------------------------

            ArrayNode baseFieldsNode = (ArrayNode) request.get("basefields");

            if (baseFieldsNode != null) {
                for (JsonNode baseFieldNode : baseFieldsNode) {

                    this.baseFields.add(new BaseField(baseFieldNode));
                }
            }
            //---------------------------------------

            JsonNode rowKeyNode = request.get("rowkey");

            if (rowKeyNode != null) {
                this.rowKeyVersion = rowKeyNode.get("version").getIntValue();
                if (rowKeyVersion == 1) {
                    ArrayNode nodes = (ArrayNode) rowKeyNode.get("keys");
                    for (JsonNode node : nodes)
                        keyFields.add(new KeyField(node));
                }
            } else {
                this.rowKeyVersion = 0;
            }
            //---------------------------------------

            String checkResult = checkFields();
            if (null != checkResult)
                throw new ServiceException(checkResult, ErrCode.PARSE_ERROR);


        } catch (ServiceException se) {
            throw se;

        } catch (Exception e) {
            e.printStackTrace();
            throw new ServiceException("request is not valid", ErrCode.PARSE_ERROR);
        }
    }

    private String checkFields() {

        for (QueryField qf : queryFields) {
            if (qf.name.equals(contentField) || fields.containsKey(qf.name)) {
                continue;
            }
            return "the query field  " + qf.name + "is not in the fields";
        }

        for (BaseField bf : baseFields) {
            if (bf.name.equals("id") || fields.containsKey(bf.name)) {
                if (bf.isFast)
                    fields.get(bf.name).stored = true;
                continue;
            }
            return "the query field " + bf.name + "is not in the fields";
        }
        return null;
    }

    public List<Element> getSolrFields() {
        List<Element> solrFields = new ArrayList<Element>();

        Element eleField;
        for (Field field : fields.values()) {

            if ((eleField = asSolrField(field)) != null) solrFields.add(eleField);
        }

        if (contentField != null && contentType != null) {

            solrFields.add(getContentField());

            for (Field field : fields.values()) {
                if ((eleField = asSolrCopyField(field)) != null) solrFields.add(eleField);
            }

        }
        return solrFields;
    }


    public Set<String> getSolrFieldNames() {

        Set<String> solrFields = new HashSet<>();

        for (Field field : fields.values()) {
            if (asSolrField(field) != null) solrFields.add(field.name);
        }

        if (contentField != null && contentType != null) {

            solrFields.add(contentField);

        }
        return solrFields;
    }

    /**
     * generate a solr field
     *
     * @return
     */
    private Element asSolrField(Field field) {

        if (field.indexed || field.contented || field.stored) {

            Element fieldEle = new DefaultElement("field");
            fieldEle.add(new DefaultAttribute("name", field.name));
            fieldEle.add(new DefaultAttribute("indexed", String.valueOf(field.indexed)));
            fieldEle.add(new DefaultAttribute("stored", String.valueOf(field.stored)));

            if (field.type == FieldType.NETSTED) {
//            field.add(new DefaultAttribute("multiValued",String.valueOf(true)));
            } else if (field.type == FieldType.ATTACHMENT) {
                fieldEle.add(new DefaultAttribute("multiValued", String.valueOf(true)));
                fieldEle.add(new DefaultAttribute("type", "string"));
            } else {
                fieldEle.add(new DefaultAttribute("type", field.type.getValue()));
            }

            return fieldEle;
        }

        return null;
    }

    /**
     * generate a solr field
     *
     * @return
     */
    private Element asSolrCopyField(Field field) {

        if (field.contented) {

            Element fieldEle = new DefaultElement("copyField");
            fieldEle.add(new DefaultAttribute("src", field.name));
            fieldEle.add(new DefaultAttribute("dst", contentField));

            return fieldEle;
        }
        return null;
    }

    private Element getContentField() {

        if (contentType != null && contentField != null) {

            Element fieldEle = new DefaultElement("field");
            fieldEle.add(new DefaultAttribute("name", this.contentField));
            fieldEle.add(new DefaultAttribute("indexed", String.valueOf(true)));
            fieldEle.add(new DefaultAttribute("stored", String.valueOf(false)));
            fieldEle.add(new DefaultAttribute("type", contentType.getValue()));

            return fieldEle;
        }
        return null;
    }

    /**
     * {"name", "hbase_table", "solr_collection",
     * "store_type",  "store_period","partition_field","content_field","content_type","rowkey_version"}
     *
     * @return
     */
    public Map<String, Object> getTableFields() {

        Map<String, Object> tableMap = new HashMap<String, Object>();
        tableMap.put("name", name);
        tableMap.put("hbase_table", hbaseTbale);
        tableMap.put("solr_collection", solrCollection);
        tableMap.put("store_type", storeType);
        tableMap.put("store_period", storePeriod);
        tableMap.put("partition_field", partition);
        tableMap.put("content_field", contentField);
        tableMap.put("content_type", contentType.getValue());
        tableMap.put("rowkey_version", rowKeyVersion);

        return tableMap;
    }

    /**
     * {"name", "indexed", "contented","stored", "hbase_column", "hbase_family","field_type","table_name"};
     *
     * @return
     */
    public List<Map<String, Object>> getSchemaFields() {

        List<Map<String, Object>> schemas = new ArrayList<Map<String, Object>>(fields.size());

        for (Field f : fields.values()) {
            Map fieldMap = new HashMap();
            fieldMap.put("name", f.name);
            fieldMap.put("indexed", String.valueOf(f.indexed));
            fieldMap.put("contented", String.valueOf(f.stored));
            fieldMap.put("stored", String.valueOf(f.stored));
            fieldMap.put("hbase_column", f.column);
            fieldMap.put("hbase_family", f.family);
            fieldMap.put("field_type", f.type.getValue());
            fieldMap.put("table_name", name);
            schemas.add(fieldMap);
        }
        return schemas;
    }

    /**
     * {"name", "is_fast", "table_name"};
     *
     * @return
     */
    public List<Map<String, Object>> getBaseFields() {

        List<Map<String, Object>> bases = new ArrayList<Map<String, Object>>(baseFields.size());

        for (BaseField f : baseFields) {
            Map fieldMap = new HashMap();
            fieldMap.put("name", f.name);
            fieldMap.put("is_fast", String.valueOf(f.isFast));

            fieldMap.put("table_name", name);
            bases.add(fieldMap);
        }

        return bases;
    }

    /**
     * {"name", "weight", "table_name"};
     *
     * @return
     */
    public List<Map<String, Object>> getQueryFields() {
        List<Map<String, Object>> queries = new ArrayList<Map<String, Object>>(queryFields.size());

        for (QueryField f : queryFields) {
            Map fieldMap = new HashMap();
            fieldMap.put("name", f.name);
            fieldMap.put("weight", f.weight);

            fieldMap.put("table_name", name);
            queries.add(fieldMap);
        }

        return queries;
    }

    /**
     * {"name", "order", "table_name"};
     *
     * @return
     */
    public List<Map<String, Object>> getKeyFields() {

        List<Map<String, Object>> keys = new ArrayList<Map<String, Object>>(keyFields.size());

        for (KeyField f : keyFields) {
            Map fieldMap = new HashMap();
            fieldMap.put("name", f.name);
            fieldMap.put("field_order", f.order);
            fieldMap.put("table_name", name);
            keys.add(fieldMap);
        }

        return keys;

    }

    public Set<byte[]> getHbaseFamilies() {
        Set<String> families=fields.values().stream().map(field->field.family).collect(Collectors.toSet());

        return  families.stream().map(family-> Bytes.toBytes(family)).collect(Collectors.toSet());
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

        fields.values().stream().filter(field -> field.indexed).forEach(field -> {

            ObjectNode fieldNode = factory.objectNode();

            fieldNode.put("inputColumn", StringUtils.join(field.family, ":", field.column));

            fieldNode.put("outputField", field.name);

            if (field.type == FieldType.NETSTED) {
                fieldNode.put("type", "com.ngdata.hbaseindexer.parse.JsonByteArrayValueMapper");
            } else if (field.type == FieldType.ATTACHMENT) {
                fieldNode.put("type", "string");
            } else {
                fieldNode.put("type", field.type.getValue());
            }

            fieldNode.put("source", "value");

            indexerFields.add(fieldNode);

        });
        return indexerFields;
    }


    class QueryField implements Serializable {

        public QueryField(JsonNode jsonNode) {

            this.name = jsonNode.get("name").getTextValue();

            this.weight = jsonNode.get("weight").getIntValue();
        }

        String name;
        int weight;
    }


    class BaseField implements Serializable {

        public BaseField(JsonNode jsonNode) {

            this.name = jsonNode.get("name").getTextValue();

            this.isFast = jsonNode.get("isFast").getBooleanValue();
        }

        String name;
        boolean isFast;
    }


    class KeyField implements Comparable<KeyField> {

        String name;
        int order;

        KeyField(JsonNode jsonNode) {

            name = jsonNode.get("name").getTextValue();
            order = jsonNode.get("order").getIntValue();
        }

        public int compareTo(KeyField o) {
            return this.order - o.order;
        }
    }

}
