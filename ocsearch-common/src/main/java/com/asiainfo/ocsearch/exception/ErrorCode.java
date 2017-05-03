package com.asiainfo.ocsearch.exception;

/**
 * Created by mac on 2017/3/23.
 */
public enum ErrorCode {
    PARSE_ERROR(101),RUNTIME_ERROR(102),SCHEMA_EXIST(103);

    int code;
    ErrorCode(int code) {
        this.code=code;
    }

    public int getCode() {
        return code;
    }
}
