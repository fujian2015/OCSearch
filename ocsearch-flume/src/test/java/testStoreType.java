import org.junit.Test;

/**
 * Created by Aaron on 17/7/18.
 */
public class testStoreType {

    @Test
    public void testTypeConvert() {

        int intType;
        long longType;
        float floatType;
        double doubleType;
        boolean boolType;
        String str1 = "123";
        String str2 = "123457862658236";
        String str3 = "1.1234";
        String str4 = "0.123531251235135135141432432";
        String str5 = "";

        intType = Integer.parseInt(str1);
        longType = Long.parseLong(str2);
        floatType = Float.parseFloat(str3);
        doubleType = Double.parseDouble(str4);
        boolType = Boolean.parseBoolean(str5);


        System.out.println(intType);
        System.out.println(longType);
        System.out.println(floatType);
        System.out.println(doubleType);
        System.out.println(boolType);

        if((!str5.equals("false"))&&(!boolType)) {
            System.out.println("error");
        }

    }

}
