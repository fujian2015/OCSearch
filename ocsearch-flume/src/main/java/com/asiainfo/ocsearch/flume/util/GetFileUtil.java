package com.asiainfo.ocsearch.flume.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Aaron on 17/7/12.
 */
public class GetFileUtil {

    public static byte[] getBytesfromLocalFile(String filePath) {
        byte[] bytes = null;
        File file = new File(filePath);
        try {
            InputStream is = new FileInputStream(file);

            // 获取文件大小
            long length = file.length();

            if (length > Integer.MAX_VALUE) {

                // 文件太大，无法读取
                throw new IOException("File is to large " + file.getName());

            }

            // 创建一个数据来保存文件数据

            bytes = new byte[(int) length];

            // 读取数据到byte数组中

            int offset = 0;

            int numRead = 0;

            while (offset < bytes.length

                    && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {

                offset += numRead;

            }

            // 确保所有数据均被读取

            if (offset < bytes.length) {

                throw new IOException("Could not completely read file "
                        + file.getName());

            }

            // Close the input stream and return bytes

            is.close();
        } catch (Exception e) {

            e.printStackTrace();
        }

        return bytes;
    }

}
