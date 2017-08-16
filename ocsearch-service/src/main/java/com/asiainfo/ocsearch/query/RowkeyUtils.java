package com.asiainfo.ocsearch.query;

import com.asiainfo.ocsearch.constants.Constants;
import com.asiainfo.ocsearch.constants.OCSearchEnv;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by mac on 2017/8/10.
 */
public class RowkeyUtils {


    public static  boolean ENCRYPT_KEY=Boolean.valueOf(OCSearchEnv.getEnvValue("ENCRYPT_KEY","false"));

    static BASE64Encoder base64Encoder = new BASE64Encoder();
    static BASE64Decoder base64Decoder = new BASE64Decoder();

    public static String encodeKey(String oriId) {
        byte[] key = null;
        try {
            key = oriId.getBytes(Constants.DEFUAT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64Encoder.encode(key);
    }

    public static String encodeKey(byte[] key) {

        return base64Encoder.encode(key);
    }

    public static String decodeKey(String enId) {

        try {
            byte[] key = base64Decoder.decodeBuffer(enId);
            return new String(key, Constants.DEFUAT_CHARSET);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }

}
