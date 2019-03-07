package model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

public class Test {
    public static void main(String[] args) {
        /**
         * 静态代理模式
         */
//        Run run = new Run();
//        Car car = new Car(run);
//        car.run();
        /**
         * 动态代理模式
         */
        //被代理类
        Run run = new Run();
        InvocationHandler h = new TimeHandler(run);
        Class<?> cls = run.getClass();

        /**
         * loader 类加载器
         * interfaces 实现接口
         * h invocationHandler
         */
        Moveable m = (Moveable) Proxy.newProxyInstance(cls.getClassLoader(),
                cls.getInterfaces(),h);
        m.run();
        m.fly();
    }
}
