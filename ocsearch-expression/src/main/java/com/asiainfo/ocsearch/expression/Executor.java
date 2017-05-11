package com.asiainfo.ocsearch.expression;

import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.MapContext;

import java.util.Map;

/**
 * Created by mac on 2017/5/10.
 */
public class Executor {
    JexlExpression jexlExpression;

    public Executor(JexlExpression jexlExpression) {
        this.jexlExpression = jexlExpression;
    }

    public String evaluate(Map<String, Object> map) {
        return (String) jexlExpression.evaluate(new MapContext(map));
    }
}
