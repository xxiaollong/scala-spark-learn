package com.example.java;

import java.util.ArrayList;
import java.util.List;

/**
 * 全局变量
 */
public class GlobalVariable {
    public static void main(String[] args) {

        Variable v1 = new Variable();
        v1.list.add(1);
        Variable v2 = new Variable();
        System.out.println(v2.list.size());
        v2.list.add(10);
        System.out.println(v1.list.size());

    }

}

class Variable{
   static List<Integer> list = new ArrayList<>();

}
