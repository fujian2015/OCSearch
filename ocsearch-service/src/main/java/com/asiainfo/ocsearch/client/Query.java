package com.asiainfo.ocsearch.client;

import com.asiainfo.ocsearch.exception.ServiceException;

/**
 * Created by mac on 2017/7/24.
 */
public interface Query {
    OCResult query(OCQuery query) throws ServiceException;
}
