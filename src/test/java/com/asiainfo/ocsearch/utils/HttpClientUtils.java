package com.asiainfo.ocsearch.utils;

import com.asiainfo.ocsearch.CommonUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;

/**
 * Created by mac on 2017/4/7.
 */
public class HttpClientUtils {

    public static void main(String[] args) throws IOException {


        httpPost("http://ochadoop19:28080/OCSearch/OCSearch/createTable", CommonUtils.getRquestDemo());
    }

    public static JsonNode httpPost(String url, JsonNode jsonParam){
        return httpPost(url, jsonParam, false);
    }

    /**
     * post请求
     * @param url         url地址
     * @param jsonParam     参数
     * @param noNeedResponse    不需要返回结果
     * @return
     */
    public static JsonNode httpPost(String url,JsonNode jsonParam, boolean noNeedResponse){
        //post请求返回结果
        DefaultHttpClient httpClient = new DefaultHttpClient();
        JsonNode jsonResult = null;
//        HttpPost method = new HttpPost(url);
//        try {
//            if (null != jsonParam) {
//                //解决中文乱码问题
//                StringEntity entity = new StringEntity(jsonParam.toString(), "utf-8");
//                entity.setContentEncoding("UTF-8");
//                entity.setContentType("application/json");
//                method.setEntity(entity);
//            }
//            HttpResponse result = httpClient.execute(method);
//            url = URLDecoder.decode(url, "UTF-8");
//            /**请求发送成功，并得到响应**/
//            if (result.getStatusLine().getStatusCode() == 200) {
//                String str = "";
//                try {
//                    /**读取服务器返回过来的json字符串数据**/
//                    str = EntityUtils.toString(result.getEntity());
//                    if (noNeedResponse) {
//                        return null;
//                    }
//                    /**把json字符串转换成json对象**/
//                    jsonResult = new ObjectMapper().readTree(str);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return jsonResult;
    }

}
