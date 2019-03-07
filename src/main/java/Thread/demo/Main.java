package Thread.demo;

public class Main {
    public static void main(String[] args) {
        Person person = new Person();
        Producer p = new Producer(person);
        Consumer c = new Consumer(person);
        Thread t1 = new Thread(p);
        Thread t2 = new Thread(c);
        t1.start();
        t2.start();
    }
}
