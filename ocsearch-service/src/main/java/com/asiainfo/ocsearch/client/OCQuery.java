package com.asiainfo.ocsearch.client;


import com.alibaba.druid.util.StringUtils;

import java.util.*;

/**
 * Created by mac on 2017/7/24.
 */
public class OCQuery {

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }


    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getRowKeyPrefix() {
        return rowKeyPrefix;
    }

    public void setRowKeyPrefix(String rowKeyPrefix) {
        this.rowKeyPrefix = rowKeyPrefix;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getCursorMark() {
        return cursorMark;
    }

    public void setCursorMark(String cursorMark) {
        this.cursorMark = cursorMark;
    }

    public Set<String> getReturnNodes() {
        return returnNodes;
    }

    public void setReturnNodes(Set<String> returnNodes) {
        this.returnNodes = returnNodes;
    }

    int start = 0;
    int rows = 10;

    String query = "";
    String rowKeyPrefix = ""; //用于scan搜索
    String condition = "";
    String cursorMark = "";

    Set<String> returnNodes = new HashSet<>();

    Set<String> tables = new HashSet<>();

    List<Sort> sorts = new LinkedList<>();

    public static class Sort {

        public Sort(String field, ORDER order) {
            this.field = field;
            this.order = order;
        }

        String field;
        ORDER order;

        public Sort(String field, int order) {
            if (order == 1)
                this.order = ORDER.asc;
            else
                this.order = ORDER.desc;
            this.field = field;
        }
    }

    public static enum ORDER {
        desc,
        asc;

        private ORDER() {
        }

        public ORDER reverse() {
            return this == asc ? desc : asc;
        }
    }

    public void setSort(String field, ORDER order) {
        if (!sorts.isEmpty()) {
            sorts.clear();
        }
        sorts.add(new Sort(field, order));
    }

    /**
     * @param field
     * @param order 1 = asc ;-1 = desc
     */
    public void setSort(String field, int order) {
        if (!sorts.isEmpty()) {
            sorts.clear();
        }
        sorts.add(new Sort(field, order));
    }

    /**
     * @param field
     * @param order 1 = asc ;-1 = desc
     */
    public void addSort(String field, int order) {
        for (Sort sort : sorts) {
            if (StringUtils.equals(sort.field, field)) {
                sorts.remove(sort);
                break;
            }
        }
        sorts.add(new Sort(field, order));
    }

    public void addSort(String field, ORDER order) {
        for (Sort sort : sorts) {
            if (StringUtils.equals(sort.field, field)) {
                sorts.remove(sort);
                break;
            }
        }
        sorts.add(new Sort(field, order));
    }

    public String getSorts() {
        StringBuilder sb = new StringBuilder();
        for (Sort sort : sorts) {
            sb.append(sort.field);
            sb.append(" ");
            sb.append(sort.order == ORDER.asc ? 1 : -1);
            sb.append(",");
        }
        if (sb.length() > 0)
            sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    public void setTable(String name) {
        if (!tables.isEmpty())
            tables.clear();
        tables.add(name);
    }

    public void addTable(String name) {
        tables.add(name);
    }

    public void addFields(Collection<String> fields) {
        fields.forEach(field -> addField(field));
    }

    public void addField(String field) {
        if (field != null)
            returnNodes.add(field);
    }
}
