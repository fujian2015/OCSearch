package com.asiainfo.ocsearch.meta;

import java.util.regex.Pattern;

/**
 * Created by Aaron on 17/6/7.
 */
public class FieldTypeChecker {

    public static boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        return pattern.matcher(str).matches();
    }

    public static boolean isDouble(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
        return pattern.matcher(str).matches();
    }
}
