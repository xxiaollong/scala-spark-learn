package com.example.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Hive自定义UDF
 *
 */
public class MyUDF1 extends UDF {

    public int evaluate(int data){

        return data + 5;
    }

    public int evaluate(int data, int data2){

        return data + data2 + 5;
    }

}
