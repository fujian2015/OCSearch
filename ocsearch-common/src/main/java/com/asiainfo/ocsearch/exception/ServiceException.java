package com.asiainfo.ocsearch.exception;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;

/**
 * Created by mac on 2017/3/21.
 */
public  class ServiceException extends Exception{


    ErrorCode errCode;

    public ServiceException(Exception e, ErrorCode code){
        super(e);

    }

    public ServiceException(String message, ErrorCode code) {
        super(message);
        this.errCode=code;
    }

    public  byte[] getErrorResponse(){

        ObjectNode re= JsonNodeFactory.instance.objectNode();
        ObjectNode result= JsonNodeFactory.instance.objectNode();
        result.put("error_code",errCode.code);
        result.put("error_desc",this.getMessage());

        re.put("result",result);

        byte []data=null;
        try {
            data= new ObjectMapper().writeValueAsBytes(re);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;

    }

}
