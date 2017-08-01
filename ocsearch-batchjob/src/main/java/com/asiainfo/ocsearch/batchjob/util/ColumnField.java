package com.asiainfo.ocsearch.batchjob.util;

import com.asiainfo.ocsearch.meta.FieldType;

import java.util.ArrayList;

/**
 * Created by Aaron on 17/6/2.
 */
public class ColumnField {

    private String column;
    private ArrayList<Integer> sequence;
    private FieldType type;
    private String seperator = null;

    public ColumnField(String column, ArrayList<Integer> sequence, FieldType type) {
        this.column = column;
        this.sequence = sequence;
        this.type = type;
    }

    public ColumnField(){}

    public String getColumn() {
        return column;
    }

    public ArrayList<Integer> getSequence() {
        return sequence;
    }

    public FieldType getType() {
        return type;
    }

    public String getSeperator() {
        return seperator;
    }

    public void setSeperator(String seperator) {
        this.seperator = seperator;
    }

}

