package com.example.java;

/**
 * 类型转换测试
 * String是值传递
 * StringBuffer是引用传递
 */
public class TypeDemo {

    public static void main(String[] args) {
//        d1();
        TypeDemo t1 = new TypeDemo();
        t1.strTest();
        System.out.println("-----------");
        t1.strbfTest();
    }

    // 类型转换
    public static void d1(){
        String str = "12.5";
        // 此处报错
        Integer num = Integer.valueOf(str);
        // Double num = Double.valueOf(str);
        System.out.println(num);
    }

    // String是值传递
    public void strTest(){
        String str = "Hello";
        System.out.println("前 str=" + str);
        strTest01(str);
        System.out.println("后 str=" + str);
    }
    // String是值传递
    public void strTest01(String str){
        str = str + " World";
        System.out.println("中 str=" + str);
    }
    // StringBuffer是引用传递
    public void strbfTest(){
        StringBuffer strbf = new StringBuffer("Hello");
        System.out.println("前 str=" + strbf.toString());
        strbfTest01(strbf);
        System.out.println("后 str=" + strbf.toString());
    }
    // StringBuffer是引用传递
    public void strbfTest01(StringBuffer strbf){
        strbf.append(" World");
        System.out.println("中 str=" + strbf.toString());
    }


}
