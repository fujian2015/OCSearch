package com.asiainfo.ocsearch.transaction;

import com.asiainfo.ocsearch.common.OCSearchEnv;
import com.asiainfo.ocsearch.exception.ErrCode;
import com.asiainfo.ocsearch.exception.ServiceException;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mac on 2017/3/27.
 */
public class TransactionUtil {

    public static void serialize(String name, Transaction transaction) throws ServiceException {

        String path = OCSearchEnv.getEnvValue("work_dir", "work") + "/transaction/";

        File serializeFile = new File(path);
        if(!serializeFile.exists())
            serializeFile.mkdirs();

        ObjectOutputStream oos = null;

        try {
            oos = new ObjectOutputStream(new FileOutputStream(new File(path,name)));
            oos.writeObject(transaction);

        } catch (IOException e) {
            e.printStackTrace();

            throw new ServiceException("can not serialize the transaction", ErrCode.RUNTIME_ERROR);

        } finally {

            if (oos != null) try {
                oos.close();
            } catch (IOException e) {
            }
        }
    }

    public static Transaction deSerialize(String name) throws ServiceException {

        String path = OCSearchEnv.getEnvValue("work_dir", "work") + "/transaction/" + name;

        File serializeFile = new File(path);
        Transaction transaction = null;

        if (serializeFile.exists()) {

            ObjectInputStream ois = null;
            try {

                ois = new ObjectInputStream(new FileInputStream(serializeFile));
                transaction = (Transaction) ois.readObject();

            } catch (IOException e) {
                e.printStackTrace();

            } catch (ClassNotFoundException e) {
                e.printStackTrace();

            } finally {

                if (ois != null) try {
                    ois.close();
                } catch (IOException e) {
                }
            }
        }
        return transaction;
    }

    public static List<String> getTranactions() throws ServiceException {

        String path = OCSearchEnv.getEnvValue("work_dir", "work") + "/transaction/" ;

        File serializeDir = new File(path);
        if(serializeDir.exists()){
           return Arrays.asList(serializeDir.list());
        }
        return new ArrayList<String>(0);
    }

    public static  void deleteTranaction(String name) throws ServiceException {


        String path = OCSearchEnv.getEnvValue("work_dir", "work") + "/transaction/" + name;

        File serializeFile = new File(path);

        if (serializeFile.exists()) {
            serializeFile.delete();
        }
    }

}
