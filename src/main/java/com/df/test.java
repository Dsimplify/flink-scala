package com.df;

import java.util.Random;

public class test {
    public static void main(String[] args) throws InterruptedException {

        boolean flag = true;

        while (flag) {
            Double a = Math.random();
            System.out.println(a);
            Thread.sleep(1000L);
        }
    }
}
