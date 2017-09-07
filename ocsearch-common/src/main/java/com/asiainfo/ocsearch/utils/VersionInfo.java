package com.asiainfo.ocsearch.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by mac on 2017/9/7.
 */
public class VersionInfo {

    static final String versionInfoFile = "common-version-info.properties";


    public static void main(String args[]) throws IOException {

        Properties info = new Properties();
        InputStream in = null;
        try {
            in = Thread.currentThread().getContextClassLoader().getResourceAsStream(versionInfoFile);
            if (in == null)
                System.out.println("can not load resource:" + versionInfoFile);

            info.load(in);
        } finally {
            if (in != null)
                in.close();
        }

        System.out.println("OCSearch " + info.getProperty("version", "Unknown"));
        System.out.println("Compiled on " + info.getProperty("date", "Unknown"));

    }

}
