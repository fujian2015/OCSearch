package com.asiainfo.ocsearch.flume.util;

import com.asiainfo.ocsearch.meta.FieldType;

import java.util.ArrayList;

/**
 * Created by Aaron on 17/7/3.
 */
public class ColumnField {

    private String separater;
    private FieldType type;
    private ArrayList<String> fieldList;

    public ColumnField(String separater, FieldType type, ArrayList<String> fieldList) {
        this.separater = separater;
        this.type = type;
        this.fieldList = fieldList;
    }
    public String getSeparater() {
        return separater;
    }

    public FieldType getType() {
        return type;
    }

    public ArrayList<String> getFieldList() {
        return fieldList;
    }

}
