package com.asiainfo.ocsearch.expression;

import com.asiainfo.ocsearch.expression.annotation.DynamicProperty;
import com.asiainfo.ocsearch.expression.annotation.Name;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by mac on 2017/5/11.
 */
public class NameSpaceManager {

    static private  Map<String, Object> classMap = new HashMap<>();
    static private Map<String, List<DynamicProperty>> dynamicPropertyMap = new HashMap();

    private static void load() {

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        ServiceLoader<NameSpace> serviceLoader = ServiceLoader.load(NameSpace.class, classLoader);

        for (NameSpace func : serviceLoader) {
            try {
                String nameSpace = getNameSpace(func.getClass());
                if (nameSpace == null)
                    continue;
                classMap.put(nameSpace, func);
                dynamicPropertyMap.put(nameSpace,getDynamicProperties(func.getClass()));

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static  List<DynamicProperty> getDynamicProperties(Class<? extends NameSpace> clazz) {

        List<DynamicProperty> dynamicProperties = new ArrayList<>();

        Method methods[] = clazz.getMethods();
        for (Method method : methods) {
            DynamicProperty dp = method.getAnnotation(DynamicProperty.class);
            if (dp != null)
                dynamicProperties.add(dp);
        }
        return dynamicProperties;

    }
    private static String getNameSpace(Class<? extends NameSpace> aClass) {

        Name nameSpace = aClass.getAnnotation(Name.class);
        if (nameSpace != null) {
            return nameSpace.value();
        }
        return null;
    }

    public static  Map<String, Object> getClassMap(){
        if(classMap.isEmpty())
            load();
        return classMap;
    }
    public static  Map<String, List<DynamicProperty>> getDynamicPropertyMap(){
        if(dynamicPropertyMap.isEmpty())
            load();
        return dynamicPropertyMap;
    }

}
