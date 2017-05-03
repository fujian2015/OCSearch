package com.asiainfo.ocsearch.meta;

import java.io.Serializable;

/**
 * Created by mac on 2017/3/23.
 */
public enum FieldType implements Serializable {

    INT("int"), FLOAT("float"), BOOLEAN("boolean"), STRING("string"), TEXT("text"), NETSTED("netsted"), FILE("file"), ATTACHMENT("attachment"),NONE("none");

    private String value;


    FieldType(String value) {
        this.value = value;
    }

    public String getValue(){
        return this.value;
    }
    // Int
//            整形
//
//    Float
//            浮点类型
//
//    Boolean
//            布尔类型
//
//    String
//            字符串
//
//    Text
//            文本
//    分词字段，queryFields字段必须是Text类型
//            Netsted
//    嵌套文档
//    嵌套文档，如一条微博的评论列表
//            File
//    文件
//    文件的二进制内容（返回的时候返回一个url）
//    Attachment
//            附件列表
//    附件（文件）列表（查询的时候返回（filename/url）对），可以把文档的附件存储在这里（在hbase里边对应一个列族，文件名为列名，solr里边建立一个多值字段可供查询）

}
