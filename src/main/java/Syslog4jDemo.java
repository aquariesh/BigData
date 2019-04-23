import org.productivity.java.syslog4j.Syslog;
import org.productivity.java.syslog4j.SyslogIF;

public class Syslog4jDemo {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        // Set a Specific Host, then Log to It
        SyslogIF syslog = Syslog.getInstance("tcp");
        syslog.getConfig().setHost("10.8.250.44");
        syslog.getConfig().setPort(514);
        while (true) {
            System.out.println("+++++");
            syslog.log(0,"Today is a good day!,liuxiangke");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
