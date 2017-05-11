package com.asiainfo.ocsearch.expression;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlExpression;

/**
 * Created by mac on 2017/5/10.
 */
public class Engine {

    private static Engine instance;

    JexlEngine jexl;

    public static Engine getInstance() {

        if (instance == null)
            instance = new Engine();
        return instance;
    }

    private Engine() {

        jexl = new JexlBuilder().namespaces(NameSpaceManager.getClassMap()).create();

//        Map<String, Map<String, DynamicProperty>> dynamicPropertyMap = NameSpaceManager.getDynamicPropertyMap();
//
//        for (String namespace : dynamicPropertyMap.keySet()) {
//            Map<String, DynamicProperty> dynamicProperty=dynamicPropertyMap.get(namespace);
//            for (String method : dynamicProperty.keySet()) {
//                replaceMap.put(namespace + ":" + dynamicProperty.get(method).name(), namespace + ":" + method);
//            }
//        }
    }

    public Executor createExecutor(String expression) {
        //把method name转换成method
//        for (Map.Entry<String, String> entry : replaceMap.entrySet()) {
//            expression = expression.replace(entry.getKey(), entry.getValue());
//        }
        JexlExpression jexlExpression = jexl.createExpression(expression);
        return new Executor(jexlExpression);
    }
}
