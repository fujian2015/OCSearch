package com.asiainfo.ocsearch.expression.namespace;

import com.asiainfo.ocsearch.expression.NameSpace;
import com.asiainfo.ocsearch.expression.annotation.Argument;
import com.asiainfo.ocsearch.expression.annotation.DynamicProperty;
import com.asiainfo.ocsearch.expression.annotation.Name;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by mac on 2017/5/11.
 */
@Name("$date")
public class DateNameSpace implements NameSpace {

    @DynamicProperty(
            name = "format",
            returnType = "string",
            description = "Formats a number as a date/time according to the format specified by the argument. The argument must be a String that is a valid Java SimpleDateFormat format. The Subject is expected to be a Number that represents the number of milliseconds since Midnight GMT on January 1, 1970. The number will be evaluated using the local time zone unless specified in the second optional argument.",
            arguments = {
                    @Argument(name = "timeStamp", type = "long", description = "a timestamp"),
                    @Argument(name = "format", type = "string", description = "The format to use in the Java SimpleDateFormat syntax")
            }
    )
    public String format(long timeStamp, String format) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);

        return simpleDateFormat.format(new Date(timeStamp));
    }

    @DynamicProperty(
            name = "toDate",
            returnType = "Date",
            description = "Converts a String into a Date data type, based on the format specified by the argument. The argument must be a String that is a valid Java SimpleDateFormat syntax. The Subject is expected to be a String that is formatted according the argument.",
            arguments = {
                    @Argument(name = "time", type = "string", description = "a string time"),
                    @Argument(name = "format", type = "string", description = "The format to use in the Java SimpleDateFormat syntax")
            }
    )
    public Date toDate(String time, String format) throws ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);

        return simpleDateFormat.parse(time);
    }
    @DynamicProperty(
            name = "now",
            returnType = "string",
            description = "Formats a number(current timestamp) as a date/time according to the format specified by the argument. The argument must be a String that is a valid Java SimpleDateFormat format.",
            arguments = {
                    @Argument(name = "format", type = "string", description = "The format to use in the Java SimpleDateFormat syntax")
            }
    )
    public String now(String format){

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);

        return simpleDateFormat.format(System.currentTimeMillis());
    }

}
