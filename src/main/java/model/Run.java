package model;

public class Run implements Moveable{
    @Override
    public void run() {
        System.out.println("run run run");
    }

    @Override
    public void fly() {
        System.out.println("fly fly fly");
    }
}
