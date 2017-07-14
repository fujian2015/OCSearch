import org.junit.Test;

/**
 * Created by Aaron on 17/7/3.
 */
public class testPutGenerator {
    @Test
    public void testGeneratePut() {
        int i = 10;
        String content = "";
        while(i>0) {
            i--;
            content = content.concat("111").concat(";");
        }
        System.out.println(content);
        System.out.println(content.length());
        System.out.println(content.lastIndexOf(";"));
        content = content.substring(0,content.lastIndexOf(";"));
        System.out.println(content);
    }
}
