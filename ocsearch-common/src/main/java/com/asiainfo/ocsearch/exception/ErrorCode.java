package com.asiainfo.ocsearch.exception;

/**
 * Created by mac on 2017/3/23.
 */
public enum ErrorCode {
    PARSE_ERROR(101),RUNTIME_ERROR(102),SCHEMA_EXIST(103), TABLE_NOT_EXIST(104), SCHEMA_NOT_EXIST(105), SCHEMA_IN_USE(106), FILE_NOT_EXISTS(107), TABLE_EXIST(108), INDEXER_EXIST(109), INDEXER_NOT_EXIST(110);

    int code;
    ErrorCode(int code) {
        this.code=code;
    }

    public int getCode() {
        return code;
    }
}
