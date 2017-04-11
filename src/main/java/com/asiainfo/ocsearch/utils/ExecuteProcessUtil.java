package com.asiainfo.ocsearch.utils;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by mac on 2017/4/5.
 */
public class ExecuteProcessUtil {

    public synchronized static String  execute(String command, File worDir, Logger logger) throws IOException, InterruptedException {

        Process p = Runtime.getRuntime().exec(command, null, worDir);

        StreamGobbler outGobbler =new StreamGobbler(p.getInputStream(), "Out", logger);
        StreamGobbler errGobbler = new StreamGobbler(p.getErrorStream(), "Error", logger);
        errGobbler.start();
        outGobbler.start();
        if (p.waitFor() != 0)
            throw new IOException(errGobbler.getResonse());
        return outGobbler.getResonse();
    }

    static class StreamGobbler extends Thread {

        StringBuilder stringBuilder;
        InputStream is;
        String type;
        Logger logger;

        final int MAX_ERROR_LENGTH=1024;

        public StreamGobbler(InputStream is, String type, Logger logger) {
            this.is = is;
            this.type = type;
            this.logger = logger;
            stringBuilder = new StringBuilder(MAX_ERROR_LENGTH);
        }

        public void run() {
            try {
                InputStreamReader isr = new InputStreamReader(is);
                BufferedReader br = new BufferedReader(isr);
                String line = null;
                if (type.equals("Error")) {
                    while ((line = br.readLine()) != null) {
                        logger.error(line);

                        if(stringBuilder.length()>=MAX_ERROR_LENGTH){
                            stringBuilder.delete(0,line.length());
                            stringBuilder.append(line);
                            stringBuilder.append("\n");
                        }
                    }
                } else {
                    while ((line = br.readLine()) != null) {
                        logger.info(line);
                    }
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
                logger.error(ioe);
                throw new RuntimeException("read "+type+" response failure",ioe);
            } finally {
                try {
                    is.close();
                } catch (Exception e) {
                    logger.error(e);
                }
            }
        }
        public String getResonse() {
            return stringBuilder.toString();
        }
    }
}
