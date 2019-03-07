package model;

public class Singleton {
    /**
     * 饿汉式单例
     */
//    private final static Singleton instance = new Singleton();
//    private Singleton(){
//
//    }
//    public static Singleton getInstance(){
//        return instance;
//    }
    /**
     * 懒汉单线程单单例
     */
//    private static Singleton instance;
//    private Singleton(){
//
//    }
//    public static Singleton getInstance(){
//        if(instance ==null){
//            instance = new Singleton();
//        }
//        return instance;
//    }
    /**
     * 双重检查模式 适合多线程 线程安全
     */
    private static Singleton instance;
    private Singleton(){

    }
    public static Singleton getInstance(){
        if(instance ==null){
            synchronized(Singleton.class){
                if (instance ==null){
                    instance = new Singleton();
                }
            }
        }
        return instance;
    }
}
