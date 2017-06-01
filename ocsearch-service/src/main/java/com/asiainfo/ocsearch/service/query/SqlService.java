package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.datasource.jdbc.HbaseJdbcHelper;
import com.asiainfo.ocsearch.datasource.jdbc.phoenix.PhoenixJdbcHelper;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by mac on 2017/6/1.
 */
public class SqlService extends QueryService {
    @Override
    protected JsonNode query(JsonNode request) throws ServiceException {

        HbaseJdbcHelper jdbcHelper = null;
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode returnData = factory.objectNode();

        ArrayNode results = factory.arrayNode();
        returnData.put("docs", results);
        try {
            jdbcHelper = new PhoenixJdbcHelper();
            String sql = request.get("sql").asText();

            ResultSet rsResultSet = jdbcHelper.executeQueryRaw(sql);

            int columnCount = rsResultSet.getMetaData().getColumnCount();
            int count = 0;
            while (rsResultSet.next()) {
                count++;
                ObjectNode objectNode = factory.objectNode();
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 1; i <= columnCount; i++) {
                    if(rsResultSet.getObject(i)==null)
                        continue;
                    objectNode.put(rsResultSet.getMetaData().getColumnName(i), toValueNode(rsResultSet.getObject(i)));
                }
                results.add(objectNode);
            }
            returnData.put("total", count);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            if (jdbcHelper != null) {
                try {
                    jdbcHelper.close();
                } catch (SQLException e1) {
                }
            }
        }
        return returnData;
    }

    private ValueNode toValueNode(Object value) {

        System.out.println(value);
        if (value instanceof Integer) {
            return new IntNode((Integer) value);
        } else if (value instanceof Double || value instanceof Float) {
            return new DoubleNode((Double) value);
        } else if (value instanceof String) {
            return new TextNode((String) value);
        } else if (value instanceof Boolean) {
            return BooleanNode.valueOf((Boolean) value);
        } else {
            return new BinaryNode((byte[]) value);
        }
    }
}
