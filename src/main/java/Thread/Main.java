package Thread;

public class Main {
    public static void main(String[] args) {
//        SellThread sellThread1 = new SellThread("A窗口");
//        SellThread sellThread2 = new SellThread("B窗口");
//        SellThread sellThread3 = new SellThread("C窗口");
//        sellThread1.start();
//        sellThread2.start();
//        sellThread3.start();
        SellRunnable r = new SellRunnable();
        Thread t1 = new Thread(r,"A窗口");
        Thread t2 = new Thread(r,"B窗口");
        Thread t3 = new Thread(r,"C窗口");
        t1.start();
        t2.start();
        t3.start();
    }
}
