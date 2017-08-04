package com.asiainfo.ocsearch.service.query;

import com.asiainfo.ocsearch.datasource.jdbc.HbaseJdbcHelper;
import com.asiainfo.ocsearch.datasource.jdbc.phoenix.PhoenixJdbcHelper;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.*;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by mac on 2017/6/1.
 */
public class SqlService extends QueryService {
    @Override
    public JsonNode query(JsonNode request) throws ServiceException {

        HbaseJdbcHelper jdbcHelper = null;
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ObjectNode returnData = factory.objectNode();

        ArrayNode results = factory.arrayNode();
        returnData.put("docs", results);
        try {
            if (false == request.has("sql"))
                throw new ServiceException("the sql service request must have 'sql' param key!", ErrorCode.PARSE_ERROR);

            String sql = request.get("sql").asText();

            jdbcHelper = new PhoenixJdbcHelper();
            ResultSet rsResultSet = jdbcHelper.executeQueryRaw(sql);

            int columnCount = rsResultSet.getMetaData().getColumnCount();
            int count = 0;
            while (rsResultSet.next()) {
                count++;
                ObjectNode objectNode = factory.objectNode();
                for (int i = 1; i <= columnCount; i++) {
                    if (rsResultSet.getObject(i) == null)
                        continue;
                    objectNode.put(rsResultSet.getMetaData().getColumnName(i), toValueNode(rsResultSet.getObject(i)));
                }
                results.add(objectNode);
            }
            returnData.put("total", count);
        } catch (ServiceException e) {
            log.warn(e);
            throw e;
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        } finally {
            if (jdbcHelper != null) {
                try {
                    jdbcHelper.close();
                } catch (SQLException e1) {
                    log.error(e1);
                }
            }
        }
        return returnData;
    }

    private ValueNode toValueNode(Object value) {

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
