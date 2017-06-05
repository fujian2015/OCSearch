import com.asiainfo.ocsearch.datainput.util.ColumnField;
import com.asiainfo.ocsearch.datainput.util.ColumnMapConverter;
import com.asiainfo.ocsearch.meta.FieldType;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Aaron on 17/6/2.
 */
public class testColumnMapConvert {
    @Test
    public void testConvert()
    {
        Map<String,ColumnField> columnFamilyMap = new HashMap<>();
        String column = "0";
        ArrayList<Integer> sequence = new ArrayList<>();
        sequence.add(1);
        FieldType type = FieldType.STRING;
        ColumnField columnField1 = new ColumnField(column,sequence,type);
        columnFamilyMap.put("B+0",columnField1);

        ArrayList<Integer> sequence1 = new ArrayList<>();
        sequence1.add(2);
        sequence1.add(3);
        String seperator = ";";
        ColumnField columnField2 = new ColumnField("1",sequence,type);
        columnField2.setSeperator(seperator);
        columnFamilyMap.put("B+1",columnField2);

        ColumnMapConverter columnMapConverter = ColumnMapConverter.getInstance();
        String str = columnMapConverter.map2String(columnFamilyMap);
        System.out.println(columnFamilyMap.toString());
        System.out.println(str);

        Map<String,ColumnField> resultMap = columnMapConverter.readJson2Map(str);
        System.out.println(resultMap.toString());
        System.out.println(resultMap.get("B+1").getColumn());

    }
    @Test
    public void testConvert2()
    {
        Map<String,ColumnField> columnFamilyMap = new HashMap<>();
        String column = "0";
        ArrayList<Integer> sequence = new ArrayList<>();
        sequence.add(1);
        FieldType type = FieldType.STRING;
        ColumnField columnField1 = new ColumnField(column,sequence,type);
        columnFamilyMap.put("B+0",columnField1);

        ArrayList<Integer> sequence1 = new ArrayList<>();
        sequence1.add(2);
        sequence1.add(3);
        String seperator = ";";
        ColumnField columnField2 = new ColumnField("1",sequence,type);
        columnField2.setSeperator(seperator);
        columnFamilyMap.put("B+1",columnField2);

        ColumnMapConverter columnMapConverter = ColumnMapConverter.getInstance();
        Gson gson = new Gson();
        String str = gson.toJson(columnFamilyMap);
        System.out.println(str);

        Map<String,ColumnField> resultMap = gson.fromJson(str,new TypeToken<Map<String,ColumnField>>(){}.getType());
        System.out.println(resultMap.toString());
        System.out.println(resultMap.get("B+1").getSeperator());
    }

}
