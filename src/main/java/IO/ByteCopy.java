package IO;

import java.io.*;

/**
 * 利用字节流完成文件复制demo
 * author：wangjx
 */
public class ByteCopy {
    public static void main(String[] args) {
        //1、创建源和目标
        File srcFile = new File("");
        File desFile = new File("");
        InputStream in = null ;
        OutputStream out = null;
        try {
            //2、创建输入输出流对象
             in = new FileInputStream(srcFile);
             out = new FileOutputStream(desFile);
            //创建一个容量为 10 的字节数组，存储已经读取的数据
             byte[] buffer = new byte[10];
            //表示已经读取了多少个字节，如果是-1，表示已经读取到文件的末尾
             int length = -1;
             while ((length=in.read(buffer))!=-1){
                 //打印读取的数据
                 System.out.println(new String(buffer,0,length));
                 //将 buffer 数组中从 0 开始，长度为 len 的数据读取到 b.txt 文件中
                 out.write(buffer,0,length);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
