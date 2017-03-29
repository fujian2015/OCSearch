package com.asiainfo.ocsearch.core;

import com.asiainfo.ocsearch.exception.ErrCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.dom4j.Element;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultElement;

import java.io.Serializable;
import java.util.*;

/**
 * Created by mac on 2017/3/24.
 */
public class TableConfig implements Serializable {

    public final String storeType;
    public final int storePeriod;
    public final String hbaseTbale;
    public final String solrCollection;
    public final String name;

    public final boolean hbaseExist;

    public final boolean solrExist;

    public final String contentField;  //default query field

    public final FieldType contentType;  //default query field


    public Map<String, Field> fields = new HashMap<String, Field>();

    public List<QueryField> queryFields = new ArrayList<QueryField>();

    public List<BaseField> baseFields = new ArrayList<BaseField>();


    public TableConfig(JsonNode request) throws ServiceException {

        try {

            this.name = request.get("name").getTextValue();

            this.storeType = request.get("storeType").getTextValue();

            if (!StringUtils.equals(this.storeType, "n"))
                this.storePeriod = request.get("storePeriod").getIntValue();
            else
                this.storePeriod = -1;

            ////
            if (request.get("contentField") != null)
                this.contentField = request.get("contentField").getTextValue();
            else
                this.contentField = null;

            if (request.get("contentType") != null)
                this.contentType = FieldType.valueOf(request.get("contentType").getTextValue().toUpperCase());
            else
                this.contentType = FieldType.NONE;
            ///

            JsonNode hbaseNode = request.get("hbase");
            if (hbaseNode != null) {
                this.hbaseTbale = hbaseNode.get("name").getTextValue();
                this.hbaseExist = hbaseNode.get("isExist").getBooleanValue();
            } else {
                this.hbaseTbale = this.name;
                this.hbaseExist = false;
            }

            JsonNode solrNode = request.get("solr");

            if (solrNode != null) {

                this.solrCollection = solrNode.get("name").getTextValue();
                this.solrExist = hbaseNode.get("isExist").getBooleanValue();
            } else {
                this.solrCollection = this.name;
                this.solrExist = false;
            }

            ArrayNode fieldsNode = (ArrayNode) request.get("fields");

            for (JsonNode fieldNode : fieldsNode) {

                Field field = new Field(fieldNode, this.hbaseExist);
                fields.put(field.name, field);
            }


            ArrayNode queryFieldsNode = (ArrayNode) request.get("queryFields");

            if (queryFieldsNode != null) {
                for (JsonNode queryFieldNode : queryFieldsNode) {

                    this.queryFields.add(new QueryField(queryFieldNode));
                }
            }


            ArrayNode baseFieldsNode = (ArrayNode) request.get("baseFields");

            if (baseFieldsNode != null) {
                for (JsonNode baseFieldNode : baseFieldsNode) {

                    this.baseFields.add(new BaseField(baseFieldNode));
                }
            }

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

    class UniqueKey implements Serializable {

        int type = 0;   //0 自动生成id 1 指定id字段  2 指定生成规则
        boolean isFast;
        String uniqueField;

        Set<KeyField> keyFields = new TreeSet<KeyField>();

        public UniqueKey(JsonNode jsonNode) {

            this.type = jsonNode.get("type").getIntValue();

            if (type == 1) {
                uniqueField = jsonNode.get("uniqueKey").getTextValue();
            } else if (type == 2) {
                ArrayNode nodes = (ArrayNode) jsonNode.get("keyFields");

                for (JsonNode node : nodes)
                    keyFields.add(new KeyField(node));
            }

            this.isFast = jsonNode.get("isFast").getBooleanValue();

        }

        class KeyField implements Comparator<KeyField> {

            String name;
            int order;

            KeyField(JsonNode jsonNode) {

                name = jsonNode.get("name").getTextValue();
                order = jsonNode.get("order").getIntValue();
            }

            public int compare(KeyField o1, KeyField o2) {
                return o1.order - o2.order;
            }
        }
    }

}
