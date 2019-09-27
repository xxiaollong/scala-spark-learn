package com.example.java;

/**
 * Java 动态绑定demo
 * 1、如果调用的是方法，则JVM将该方法和对应的对象的内存地址绑定
 * 2、如果调用的是属性，则没有动态绑定，在哪里调用，则使用哪里的值
 */
public class DynamicBindingDemo {
    public static void main(String[] args) {
        // a1实际的内存地址为B1对象
        A1 a1 = new B1();
        // 在B1中调用sum()：20 + 20
        System.out.println(a1.sum());
        // 在B1中调用sum_1()：20 + 15
        System.out.println(a1.sum_1());
        System.out.println("--------------");

        // a2实际的内存地址为B2对象
        A1 a2 = new B2();
        // 在A1中调用sum()：10 + 10
        System.out.println(a2.sum());
        // 在A1中调用sum_1(),但是getI()在B2中调用：20 + 5
        System.out.println(a2.sum_1());
    }
}

class A1{
    int i = 10;

    public int sum(){
        return i + 10;
    }
    public int sum_1(){
        return getI() + 5;
    }
    public int getI(){
        return i;
    }
}

class B1 extends A1{
    int i = 20;

    public int sum(){
        return i + 20;
    }
    public int sum_1(){
        return getI() + 15;
    }
    public int getI(){
        return i;
    }
}

class B2 extends A1{
    int i = 20;

    public int getI(){
        return i;
    }
}
