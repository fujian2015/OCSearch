package com.asiainfo.ocsearch.utils;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Created by mac on 2017/3/22.
 */
public class PropertiesLoadUtil {
    /**
     * load properties from properties file
     * @param fileName
     * @return
     */
    public static Properties loadProFile(String fileName) {

        Properties props = new Properties();
        InputStream in = null;
        try {
            in = PropertiesLoadUtil.class.getClassLoader().getResourceAsStream(fileName);
            props.load(in);

        } catch (Exception e) {
            props = null;
        } finally {
            try {
                if (in != null)
                    in.close();
            } catch (Exception e) {
            }
        }
        return props;
    }

    /**
     * load properties from xml file
     * @param fileName
     * @return
     */
    public static Properties loadXmlFile(String fileName) {
        Properties props = new Properties();
        InputStream in = null;
        try {
            in = PropertiesLoadUtil.class.getClassLoader().getResourceAsStream(fileName);

            SAXReader sr = new SAXReader();
            Document schemaDoc = sr.read(in);

            Element root = schemaDoc.getRootElement();
            List<Element> propertyEles = root.elements("property");
            for (Element ele : propertyEles) {
                String name = ele.element("name").getTextTrim();
                String value = ele.element("value").getTextTrim();
                props.put(name, value);
            }
        } catch (Exception e) {
            props = null;
        } finally {
            try {
                if (in != null)
                    in.close();
            } catch (Exception e) {
            }
        }
        return props;
    }


    public static String loadFile(String fileName) {

        return PropertiesLoadUtil.class.getClassLoader().getResource(fileName).getFile();
    }


}
