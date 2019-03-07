package Thread;

public class SellThread extends Thread {
        //定义一共有 50 张票，注意声明为 static,表示几个窗口共享
    private static int num = 50;

    //调用父类构造方法，给线程命名
    public SellThread(String string) {
        super(string);
    }

    @Override
    public void run() {
        for (int i = 0; i < 50; i++) {
            synchronized (this.getClass()) {
                if (num > 0) {
                    try {
                        sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println(this.currentThread().getName() + "卖出一张票，剩余" + (--num) + "张");
                }
            }
        }
    }
}
