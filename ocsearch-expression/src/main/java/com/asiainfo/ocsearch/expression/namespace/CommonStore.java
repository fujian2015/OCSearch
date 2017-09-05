package com.asiainfo.ocsearch.expression.namespace;

import com.asiainfo.ocsearch.expression.NameSpace;
import com.asiainfo.ocsearch.expression.annotation.Argument;
import com.asiainfo.ocsearch.expression.annotation.DynamicProperty;
import com.asiainfo.ocsearch.expression.annotation.Name;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.UUID;

/**
 * Created by mac on 2017/5/11.
 */
@Name("$common")
public class CommonStore implements NameSpace {

    @DynamicProperty(
            name = "uuid",
            returnType = "string",
            description = "Returns a randomly generated UUID.",
            arguments = {
            }
    )
    public String uuid() {
        return UUID.randomUUID().toString();
    }

    Random random = new Random();

    @DynamicProperty(
            name = "nextInt",
            returnType = "int",
            description = "Returns a pseudo random, uniformly distributed int value between 0  and the specified value ",
            arguments = {
                    @Argument(name = "bound", type = "int", description = "the upper bound ")
            }
    )
    public int nextInt(int bound) {
        return random.nextInt(bound);
    }


    private MessageDigest md = null;

    @DynamicProperty(
            name = "md5Prefix",
            returnType = "String",
            description = "为字符串生成一个长度为3的(md5)随机字符串：将原始字符串转换成取md5值，生成16进制字符串，取第1，3，5位，",
            arguments = {
                    @Argument(name = "oriStr", type = "String", description = "原始rowkey")
            }
    )
    public String md5Prefix(String oriStr) {
        if (oriStr == null) {
            throw new RuntimeException("param of oriRowKey is null");
        }
        try {
            if (this.md == null) {
                //MD5算法
                this.md = MessageDigest.getInstance("MD5");
            }
            byte[] digest = this.md.digest(oriStr.getBytes("UTF-8"));

            StringBuffer sb = new StringBuffer();
            for (byte b : digest) {
                //转为十六进制字符串
                sb.append(Integer.toHexString(b & 0xFF));
            }
            String result = sb.toString();
            //取字符串的1、3、5位
            return result.substring(1, 2).concat(result.substring(3, 4)).concat(sb.toString().substring(5, 6));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

}
