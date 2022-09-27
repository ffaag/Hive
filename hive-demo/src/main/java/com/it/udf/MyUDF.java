package com.it.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author ZuYingFang
 * @time 2021-12-31 11:49
 * @description
 */
public class MyUDF extends GenericUDF {


    /**
     * 校验数据参数个数类型等
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {

        if(objectInspectors.length != 1){
            throw new UDFArgumentException("参数个数不唯一");
        }

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }


    /**
     * 处理数据
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        //1 取出输入数据
        String input = deferredObjects[0].get().toString();

        //2 判断输入数据是否为null
        if (input == null) {
            return 0;
        }

        //3 输入数据的长度
        return input.length();



    }

    @Override
    public String getDisplayString(String[] strings) {
        return "";
    }
}
