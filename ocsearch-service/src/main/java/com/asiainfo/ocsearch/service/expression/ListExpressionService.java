package com.asiainfo.ocsearch.service.expression;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.expression.NameSpaceManager;
import com.asiainfo.ocsearch.expression.annotation.Argument;
import com.asiainfo.ocsearch.expression.annotation.DynamicProperty;
import com.asiainfo.ocsearch.service.OCSearchService;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.util.List;
import java.util.Map;

/**
 * Created by mac on 2017/6/12.
 */
public class ListExpressionService extends OCSearchService {
    @Override
    public byte[] doService(JsonNode request) throws ServiceException {
        try {
            Map<String, List<DynamicProperty>> dynamicPropertyMap = NameSpaceManager.getDynamicPropertyMap();

            JsonNodeFactory factory = JsonNodeFactory.instance;

            ObjectNode expressions = factory.objectNode();
            dynamicPropertyMap.entrySet().forEach(entry -> {
                ArrayNode funcs = factory.arrayNode();

                expressions.put(entry.getKey(), funcs);

                entry.getValue().forEach(prop -> {

                    ObjectNode func = factory.objectNode();
                    funcs.add(func);
                    func.put("name", prop.name());
                    func.put("description", prop.description());
                    func.put("return_type", prop.returnType());
                    ArrayNode args = factory.arrayNode();
                    func.put("args", args);
                    for (Argument argument : prop.arguments()) {
                        ObjectNode arg = factory.objectNode();
                        args.add(arg);
                        arg.put("description", argument.description());
                        arg.put("name", argument.name());
                        arg.put("type", argument.type());
                    }
                });
            });

            ObjectNode successResult = getSuccessResult();
            successResult.put("expressions", expressions);
            return successResult.toString().getBytes(Constants.DEFUAT_CHARSET);
        } catch (Exception e) {
            log.error(e);
            throw new ServiceException(e, ErrorCode.RUNTIME_ERROR);
        }
    }
}
