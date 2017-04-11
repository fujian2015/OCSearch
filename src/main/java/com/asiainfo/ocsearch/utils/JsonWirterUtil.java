package com.asiainfo.ocsearch.utils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by mac on 2017/4/5.
 */
public class JsonWirterUtil {

    static final String NEW_LINE = "\n";
    static final String TAB = "\t";
    static final String QUOT = "\"";
    static final String indents = "\t\t\t\t\t\t\t\t\t";

    public static String toConfigString(JsonNode node, int len) {

        StringBuilder sb = new StringBuilder();
        String indent = indents.substring(0, len);
        String indent1 = indents.substring(0, len + 1);

        if (node instanceof ArrayNode) {

            sb.append(TAB + "[");

            for (JsonNode jsonNode : (ArrayNode) node) {

                sb.append(NEW_LINE + indent);
                sb.append(toConfigString(jsonNode, len + 1));
            }
            sb.append(NEW_LINE + indent + "]");

        } else if (node instanceof ObjectNode) {

            Iterator<Map.Entry<String, JsonNode>> it = ((ObjectNode) node).getFields();

            sb.append(TAB + "{");

            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();

                sb.append(NEW_LINE + indent1 + entry.getKey());

                if (entry.getValue() instanceof ObjectNode) {
                    sb.append(TAB);
                } else {
                    sb.append("\t:");
                }

                sb.append(toConfigString(entry.getValue(), len + 1));
            }
            sb.append(NEW_LINE + indent + "}");

        } else {
            sb.append(TAB + QUOT + node.asText() + QUOT);
        }
        return sb.toString();
    }
}
