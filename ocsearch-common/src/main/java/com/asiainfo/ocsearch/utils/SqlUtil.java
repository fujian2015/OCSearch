package com.asiainfo.ocsearch.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by mac on 2017/5/3.
 */
public class SqlUtil {
    public static Object[][] prepareObjects(List<Map<String, Object>> schemaList) {

        Object[][] fields = new Object[schemaList.size()][];
        int i = 0;

        for (Map<String, Object> fieldMap : schemaList) {
            fields[i++] = fieldMap.values().toArray();
        }
        return fields;
    }

    public static String prepareSql(String table, Set<String> keys) {
        StringBuilder sb = new StringBuilder("insert into ");
        sb.append(table);
        sb.append("(");
        sb.append(StringUtils.join(keys, ","));
        sb.append(")values(");
        String[] quots = new String[keys.size()];
        Arrays.fill(quots, "?");
        sb.append(StringUtils.join(quots, ","));
        sb.append(");");
        return sb.toString();
    }
}
