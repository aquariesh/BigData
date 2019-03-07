package model;

public class Car implements Moveable{

    private Moveable moveable;

    public Car(Moveable moveable){
        this.moveable=moveable;
    }

    @Override
    public void run() {
        System.out.println("car run");
    }

    @Override
    public void fly() {
        System.out.println("fly run");
    }
}
