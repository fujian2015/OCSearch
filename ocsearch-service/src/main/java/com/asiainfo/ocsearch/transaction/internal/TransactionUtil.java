package com.asiainfo.ocsearch.transaction.internal;

import com.asiainfo.ocsearch.constants.OCSearchEnv;
import com.asiainfo.ocsearch.exception.ErrorCode;
import com.asiainfo.ocsearch.exception.ServiceException;
import com.asiainfo.ocsearch.transaction.Transaction;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by mac on 2017/3/27.
 */
public class TransactionUtil {

    public static void serialize(String name, Transaction transaction, boolean isRollback) throws ServiceException {

        String path = getPath(isRollback);

        File serializeFile = new File(path);
        if (!serializeFile.exists()) {
            if (false == serializeFile.mkdirs())
                throw new ServiceException("make the transaction dir failure," + path, ErrorCode.RUNTIME_ERROR);
        }

        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(new File(path, name)));
            oos.writeObject(transaction);
        } catch (IOException e) {
            deleteTransaction(name, isRollback);
            throw new ServiceException("can not serialize the transaction:" + name, ErrorCode.RUNTIME_ERROR);
        } finally {
            if (oos != null) try {
                oos.close();
            } catch (IOException e) {
            }
        }
    }

    public static Transaction deSerialize(String name, boolean isRollback) throws ServiceException {

        String path = getPath(isRollback);

        File serializeFile = new File(path, name);
        Transaction transaction = null;

        if (serializeFile.exists()) {

            ObjectInputStream ois = null;
            try {
                ois = new ObjectInputStream(new FileInputStream(serializeFile));
                transaction = (Transaction) ois.readObject();
            } catch (Exception e) {
                throw new ServiceException("can not deserialize the transaction : " + serializeFile.getAbsolutePath(), ErrorCode.RUNTIME_ERROR);
            } finally {
                if (ois != null) try {
                    ois.close();
                } catch (IOException e) {
                }
            }
        }
        return transaction;
    }

    public static List<String> getTranactions(boolean isRollback) {

        String path = getPath(isRollback);

        File serializeDir = new File(path);
        if (serializeDir.exists()) {
            String[] files = serializeDir.list();
            if (files != null)
                return Arrays.asList(files);
        }

        return new ArrayList<>(0);
    }

    public static void deleteTransaction(String name, boolean isRollback) {

        String path = getPath(isRollback);

        File serializeFile = new File(path, name);

        if (serializeFile.exists()) {
            if(false==serializeFile.delete())
                throw  new RuntimeException("delete the serializeFile failure: "+name);
        }
    }

    private static String getPath(boolean rollBack) {

        String path = OCSearchEnv.getEnvValue("work_dir", "work") + "/transaction/";

        if (rollBack)
            path = path + "rollback/";
        else
            path = path + "process/";
        return path;
    }

}
