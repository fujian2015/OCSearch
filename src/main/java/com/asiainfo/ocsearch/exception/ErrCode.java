package com.asiainfo.ocsearch.exception;

/**
 * Created by mac on 2017/3/23.
 */
public enum ErrCode {
    PARSE_ERROR(1),RUNTIME_ERROR(2),TABLE_EXIST(3),FAMILY_NOT_MATCH(4), TABLE_NOT_EXISTS(5);

    int code;
    ErrCode(int code) {
        this.code=code;
    }

    public enum SUCCESS {}
}
