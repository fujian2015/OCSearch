import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;
import sun.misc.BASE64Decoder;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Created by Aaron on 17/7/14.
 */
public class testFileId {
    @Test
    public void testID2FileField() {
        BASE64Encoder base64Encoder = new BASE64Encoder();
        BASE64Decoder base64Decoder = new BASE64Decoder();
        String table = "ATTACHMENT__201707";
        String id = "18b38ad0-6143-4629-a4ef-24659d2452cb";
        String name = "pic";
        String file = "a3c8a22c08c93160726b3e57f4064247.jpeg";
        String oriID = generateFilelUrl(table, id,name, file);
        System.out.println("oriID = "+oriID);
        String oriId = "eyJ0IjoiQVRUQUNITUVOVF9fMjAxNzA3IiwiZiI6InBpYzphM2M4YTIyYzA4YzkzMTYwNzI2YjNl\nNTdmNDA2NDI0Ny5qcGVnIiwiciI6IjE4YjM4YWQwLTYxNDMtNDYyOS1hNGVmLTI0NjU5ZDI0NTJj\nYiJ9";
        JsonNode jsonNode = null;
        try {
            byte[] bytes = base64Decoder.decodeBuffer(oriId);
            String str = new String(bytes,"UTF-8");
            System.out.println(str);
            jsonNode = new ObjectMapper().readTree(base64Decoder.decodeBuffer(oriId));
        } catch (IOException e) {
            e.printStackTrace();
        }
        String table1 = jsonNode.get("t").asText();
        String rowKey = jsonNode.get("r").asText();
        String field = jsonNode.get("f").asText();
        System.out.println(table1);
        System.out.println(rowKey);
        System.out.println(field);
    }
    @Test
    public void testGenerateAttachmentId() {
        ArrayNode attachNode = JsonNodeFactory.instance.arrayNode();
        String valueArray = "a3c8a22c08c93160726b3e57f4064247.jpeg";
        for (String file : valueArray.split(",")) {
            String table = "ATTACHMENT__201707";
            String id = "18b38ad0-6143-4629-a4ef-24659d2452cb";
            String name = "pic";
            attachNode.add(generateFilelUrl(table, id,name, file));
        }
//        data.put(name, attachNode);
        System.out.println(attachNode);
    }
    private String generateFilelUrl(String table,  String id,String field,String file) {
        return new FileID(table,field+":"+file,id).toString();
    }

    private class FileID {


        public String getTable() {
            return table;
        }

        public String getField() {
            return field;
        }

        public String getRowKey() {
            return rowKey;
        }

        String table;
        String field;
        String rowKey;

        public FileID(String table, String field, String rowKey) {
            this.field = field;
            this.table = table;
            this.rowKey = rowKey;
        }

        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;

        public String toString() {

            BASE64Encoder base64Encoder = new BASE64Encoder();
            ObjectNode objectNode = jsonNodeFactory.objectNode();
            objectNode.put("t", table);
            objectNode.put("f", field);
            objectNode.put("r", rowKey);

            String id = "";
            try {
                id = base64Encoder.encode(objectNode.toString().getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }

            return id;
        }
//        public static FileID parseId(String oriId) throws IOException {
//
//            BASE64Decoder base64Decoder = new BASE64Decoder();
//            JsonNode jsonNode = new ObjectMapper().readTree(base64Decoder.decodeBuffer(oriId));
//            String table = jsonNode.get("t").asText();
//            String rowKey = jsonNode.get("r").asText();
//            String field = jsonNode.get("f").asText();
//
//            if (StringUtils.isEmpty(table) || StringUtils.isEmpty(rowKey) || StringUtils.isEmpty(field))
//                throw new IOException();
//            return new FileID(table, field, rowKey);
//        }

    }
}
