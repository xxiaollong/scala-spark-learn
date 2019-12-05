package com.example.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义UDTF
 *
 */
public class MyUDTF1 extends GenericUDTF{

    // 定义输出数据的列名和类型
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 输出列名
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("word");
        // 输出类型
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 写出数据集合
    private List<String> dataList = new ArrayList<>();

    @Override
    public void process(Object[] objects) throws HiveException {
        // 获取数据
        String data = objects[0].toString();

        // 获取分隔符
        String splitKey = objects[1].toString();

        // 切分数据
        String[] words = data.split(splitKey);

        // 写出数据
        for (String word : words) {
            dataList.clear();
            dataList.add(word);
            forward(dataList);
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
