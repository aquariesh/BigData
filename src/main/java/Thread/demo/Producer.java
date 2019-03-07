package Thread.demo;

public class Producer implements Runnable{
    Person p = null;

    public Producer(Person p){
        this.p = p ;
    }

    @Override
    public void run() {
        for(int i= 0;i<50;i++){
            //如果是偶数，那么生产对象Tom---11，如果是奇数，生产对象Marry---21
            if(i%2==0){
                p.push("Tom",11);
            }
            else {
                p.push("Marry",21);
            }
        }
    }
}
