package com.asiainfo.ocsearch.meta;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mac on 2017/3/23.
 */
public class Field implements Serializable, Cloneable {

    String name = "";

    String contentField;   //copy in solr field
    String innerField;
    int innerIndex = -1;

    boolean indexed = false;  //index in solr
    boolean indexStored = false;    //store in solr

    String indexType = "";

    String hbaseColumn; //hbase column

    String hbaseFamily;    //hbase column family

    FieldType storeType;

    List<Field> childFields = new ArrayList<Field>();

    /**
     * `name` varchar(255) NOT NULL,
     * `indexed` varchar(5) NOT NULL,
     * `index_contented` varchar(5) NOT NULL,
     * `index_stored` varchar(5) NOT NULL,
     * `index_type` varchar(255) NOT NULL,
     * `hbase_column` varchar(255) NOT NULL,
     * `hbase_family` varchar(255) NOT NULL,
     * `store_type` varchar(255) NOT NULL,
     * `schema_name` varchar(255) NOT NULL,
     *
     * @param field
     * @throws ServiceException
     */
    public Field(JsonNode field) throws ServiceException {

        try {
            this.name = field.get("name").asText();


            if (field.has("content_field"))
                this.contentField = field.get("content_field").asText();
            //inner field and index
            if (field.has("inner_field"))
                this.innerField = field.get("inner_field").asText();
            if (field.has("inner_index"))
                this.innerIndex = field.get("inner_index").asInt();

            this.indexed = field.get("indexed").asBoolean();

            if (field.has("index_stored")) this.indexStored = field.get("index_stored").asBoolean();

            if (field.has("index_type")) this.indexType = field.get("index_type").asText();

            this.storeType = FieldType.valueOf(field.get("store_type").asText().toUpperCase());

            if (field.has("hbase_column")) {
                this.hbaseColumn = field.get("hbase_column").asText();
                this.hbaseFamily = field.get("hbase_family").asText();
            }

//            if (storeType == FieldType.ATTACHMENT) {
//                this.hbaseFamily = ATTACHMENT_FAMILY;
//            } else if (storeType == FieldType.FILE) {
//                this.hbaseFamily = FILE_FAMILY;
//            } else {
//                this.hbaseFamily = BASIC_FAMILY;
//            }

//            if(this.type ==FieldType.NETSTED){
//                ArrayNode children= (ArrayNode) field.get("children");
//                for(JsonNode child:children){
//                    childFields.add(new Field(child));
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ServiceException("field is not valid", ErrorCode.PARSE_ERROR);
        }
    }


    public Field(String name, boolean indexed, String contented, boolean indexStored, String indexType,
                 String storeType, String hbaseColumn, String hbaseFamily, String innerField, int innerIndex) {

        this.name = name;

        this.indexed = indexed;

        this.contentField = contented;

        this.indexStored = indexStored;

        this.indexType = indexType;

        this.storeType = FieldType.valueOf(storeType);

        this.hbaseColumn = hbaseColumn;

        this.hbaseFamily = hbaseFamily;

        this.innerField = innerField;

        this.innerIndex = innerIndex;

    }


    public String getName() {
        return name;

    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isIndexed() {
        return indexed;
    }

    public void setIndexed(boolean indexed) {
        this.indexed = indexed;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public boolean isIndexStored() {
        return indexStored;
    }

    public void setIndexStored(boolean indexStored) {
        this.indexStored = indexStored;
    }

    public String getHbaseColumn() {
        return hbaseColumn;
    }

    public String getHbaseFamily() {
        return hbaseFamily;
    }

    public FieldType getStoreType() {
        return storeType;
    }


    public void setStoreType(FieldType storeType) {
        this.storeType = storeType;
    }

    public void setHbaseFamily(String hbaseFamily) {
        this.hbaseFamily = hbaseFamily;
    }

    public void setHbaseColumn(String hbaseColumn) {
        this.hbaseColumn = hbaseColumn;
    }

    public boolean withSolr() {
        return (indexed || indexStored || contentField != null);
    }

    public JsonNode toJsonNode() {

        ObjectNode fieldNode = new ObjectMapper().createObjectNode();

        fieldNode.put("name", name);
        fieldNode.put("indexed", indexed);


        fieldNode.put("index_stored", indexStored);

        fieldNode.put("index_type", indexType);

        fieldNode.put("store_type", storeType.toString());

        if (contentField != null) fieldNode.put("content_field", contentField);

        if (hbaseColumn != null) {
            fieldNode.put("hbase_column", hbaseColumn);
            fieldNode.put("hbase_family", hbaseFamily);
        }

        if (innerField != null) {
            fieldNode.put("inner_field", innerField);
            fieldNode.put("inner_index", innerIndex);
        }

        return fieldNode;
    }

    @Override
    public Object clone() {
        return new Field(name, indexed, contentField, indexStored, indexType, storeType.toString(), hbaseColumn, hbaseFamily, innerField, innerIndex);
    }

    public String getContentField() {
        return contentField;
    }

    public void setContentField(String contentField) {
        this.contentField = contentField;
    }

    public String getInnerField() {
        return innerField;
    }

    public void setInnerField(String innerField) {
        this.innerField = innerField;
    }

    public int getInnerIndex() {
        return innerIndex;
    }

    public void setInnerIndex(int innerIndex) {
        this.innerIndex = innerIndex;
    }

    public boolean isIndexContented() {
        return contentField != null;
    }

    public boolean equals(Field field) {
        if(indexEquals(field)&&
                field.getInnerIndex()==innerIndex
                &&StringUtils.equals(field.getInnerField(),innerField)
                &&StringUtils.equals(field.getHbaseColumn(),hbaseColumn)
                &&StringUtils.equals(field.getHbaseFamily(),hbaseFamily)
                &&field.getStoreType()==storeType)
            return true;
        return false;
    }

    public boolean indexEquals(Field field) {
        if (field.isIndexed() == indexed
                && field.isIndexContented() == isIndexContented()
                && field.isIndexStored() == indexStored
                && StringUtils.equals(field.getIndexType(), indexType)
                )
            return true;
        return false;
    }

}
