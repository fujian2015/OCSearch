package com.asiainfo.ocsearch.meta;

import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mac on 2017/3/23.
 */
public class Field implements Serializable {

    static final String BASIC_FAMILY = "B";
    static final String FILE_FAMILY = "C";
    static final String ATTACHMENT_FAMILY = "D";
    static final String NETSTED_FAMILY = "E";

    String name = "";

    boolean indexContented = false;   //index in solr
    boolean indexed = false;  //copy in hbase
    boolean indexStored = false;    //store in solr

    String indexType = "";

    String hbaseColumn = ""; //hbase column

    String hbaseFamily = "";    //hbase column family

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

            this.indexed = field.get("indexed").asBoolean();

            this.indexContented = field.get("index_contented").asBoolean();

            this.indexStored = field.get("index_stored").asBoolean();

            this.indexType = field.get("index_type").asText();
            this.storeType = FieldType.valueOf(field.get("store_type").asText().toUpperCase());

            this.hbaseColumn = name;

            if (storeType == FieldType.ATTACHMENT) {
                this.hbaseFamily = ATTACHMENT_FAMILY;
            } else if (storeType == FieldType.FILE) {
                this.hbaseFamily = FILE_FAMILY;
            } else {
                this.hbaseFamily = BASIC_FAMILY;
            }

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


    public Field(String name, boolean indexed, boolean indexContented, boolean indexStored, String indexType, String storeType, String hbaseColumn, String hbaseFamily) {

        this.name = name;

        this.indexed = indexed;

        this.indexContented = indexContented;

        this.indexStored = indexStored;

        this.indexType = indexType;

        this.storeType = FieldType.valueOf(storeType);

        this.hbaseColumn = hbaseColumn;

        this.hbaseFamily = hbaseFamily;

    }

    public boolean isIndexContented() {
        return indexContented;
    }

    public void setIndexContented(boolean indexContented) {
        this.indexContented = indexContented;
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
        return indexed || indexStored || indexContented;
    }

    public JsonNode toJsonNode() {
        ObjectNode fieldNode = new ObjectMapper().createObjectNode();

        fieldNode.put("name", name);
        fieldNode.put("indexed", indexed);
        fieldNode.put("index_contented", indexed);
        fieldNode.put("index_stored", indexStored);
        fieldNode.put("index_type", indexType);
        fieldNode.put("hbase_column", hbaseColumn);
        fieldNode.put("hbase_family", hbaseFamily);
        fieldNode.put("store_type", storeType.toString());

        return fieldNode;
    }

    @Override
    public Object clone() {
        return new Field(name, indexed, indexContented, indexStored, indexType, storeType.toString(), hbaseColumn, hbaseFamily);
    }
}
