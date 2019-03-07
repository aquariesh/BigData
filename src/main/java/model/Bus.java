package model;

public class Bus implements Moveable{

    private Moveable moveable;

    public Bus(Moveable moveable){
        this.moveable=moveable;
    }

    @Override
    public void run() {
        System.out.println("bus run");
    }

    @Override
    public void fly() {

    }
}
