package com.asiainfo.ocsearch.expression.namespace;

import com.asiainfo.ocsearch.expression.NameSpace;
import com.asiainfo.ocsearch.expression.annotation.Argument;
import com.asiainfo.ocsearch.expression.annotation.DynamicProperty;
import com.asiainfo.ocsearch.expression.annotation.Name;

import java.util.Random;
import java.util.UUID;

/**
 * Created by mac on 2017/5/11.
 */
@Name("$common")
public class CommonStore implements NameSpace {

    @DynamicProperty(
            name = "uuid",
            returnType = "string",
            description = "Returns a randomly generated UUID.",
            arguments = {
            }
    )
    public String uuid() {
        return  UUID.randomUUID().toString();
    }
    Random random =new Random();
    @DynamicProperty(
            name = "uuid",
            returnType = "int",
            description = "Returns a pseudo random, uniformly distributed int value between 0  and the specified value ",
            arguments = {
                    @Argument(name = "bound", type = "int", description = "the upper bound ")
            }
    )
    public int nextInt(int bound) {
        return random.nextInt(bound);
    }

}
