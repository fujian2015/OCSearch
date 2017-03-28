package com.asiainfo.ocsearch.core;

import com.asiainfo.ocsearch.exception.ErrCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.codehaus.jackson.JsonNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mac on 2017/3/23.
 */
public class Field implements Serializable {

    final String BASIC_FAMILY = "B";
    final String FILE_FAMILY = "C";
    final String ATTACHMENT_FAMILY = "D";
    final String NETSTED_FAMILY = "E";

    String name = "";

    FieldType type = FieldType.NONE;

    boolean indexed = false;
    boolean contented = false;

    boolean stored = false;

    //hbase column
    String column = "";
    //hbase column family
    String family = "";

    List<Field> childFields = new ArrayList<Field>();


    public Field(JsonNode field) throws ServiceException {

        try {
            this.name = field.get("name").asText();

            this.type = FieldType.valueOf(field.get("type").asText().toUpperCase());

            this.indexed = Boolean.valueOf(field.get("indexed").asText());

            this.contented = Boolean.valueOf(field.get("contented").asText());

//            if(this.type ==FieldType.NETSTED){
//                ArrayNode children= (ArrayNode) field.get("children");
//                for(JsonNode child:children){
//                    childFields.add(new Field(child));
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new ServiceException("field is not valid", ErrCode.PARSE_ERROR);
        }
    }

    public Field(JsonNode field, boolean withHbase) throws ServiceException {

        this(field);
        try {
            if (withHbase) {
                this.column = field.get("column").asText();

                this.family = field.get("family").asText();
                ;
            } else {
                this.family = name;
                if (type == FieldType.ATTACHMENT) {
                    this.column = ATTACHMENT_FAMILY;
                } else if (type == FieldType.FILE) {
                    this.column = FILE_FAMILY;
                } else {
                    this.column = BASIC_FAMILY;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new ServiceException("field is not valid", ErrCode.PARSE_ERROR);
        }

    }
}
