package Reflect;

import java.lang.reflect.Field;

public class Main {
    public static void main(String[] args) {
        TestReflect test = new TestReflect();
        Class<? extends TestReflect> aClass = test.getClass();
        try {
            Field name = aClass.getField("age");
            System.out.println(name);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        System.out.println(aClass);
        Class<TestReflect> testJavaClass = TestReflect.class;
        try {
            TestReflect testJava = testJavaClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        System.out.println(testJavaClass);
        try {
            Class<?> aClass1 = Class.forName("Reflect.TestReflect");
            Field[] fields = aClass1.getFields();
            for (Field s :fields
                 ) {
                System.out.println(s);
            }
            System.out.println(aClass1);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(aClass==testJavaClass);

    }
}
