package com.asiainfo.ocsearch.flume.util;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * Created by Aaron on 17/6/30.
 */
public class HttpRestFulClient {
    public static JsonNode getRequest(String targetURL) {

        JsonNode jsonNode = null;
        if(!targetURL.startsWith("http://")) {
            targetURL = "http://"+targetURL;
        }

        try {

            URL restServiceURL = new URL(targetURL);

            HttpURLConnection httpConnection = (HttpURLConnection) restServiceURL.openConnection();
            httpConnection.setRequestMethod("GET");
            httpConnection.setRequestProperty("Accept", "application/json");

            if (httpConnection.getResponseCode() != 200) {
                throw new RuntimeException("HTTP GET Request Failed with Error code : "
                        + httpConnection.getResponseCode());
            }
//
            BufferedReader responseBuffer = new BufferedReader(new InputStreamReader(
                    (httpConnection.getInputStream())));

            String line = responseBuffer.readLine();

            String jsonStr = line;
//            String jsonStr = httpConnection.getResponseMessage();
            ObjectMapper mapper = new ObjectMapper();
            jsonNode = mapper.readTree(jsonStr);

            httpConnection.disconnect();

        } catch (IOException e) {

            e.printStackTrace();

        }
        return jsonNode;

    }
}
