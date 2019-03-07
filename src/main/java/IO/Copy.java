package IO;

import java.io.*;

/**
 * 字符流文件复制demo
 * author：wangjx
 */
public class Copy {
    public static void main(String[] args) {
        //1、创建源和目标
        File srcFile = new File("");
        File descFile = new File("");
//        Reader in = null;
//        Writer out = null;
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            //2、创建字符输入输出流对象 利用缓存流 加快流读取和写入的效率
            br = new BufferedReader(new FileReader(srcFile));
            bw = new BufferedWriter(new FileWriter(descFile));
//             in = new FileReader(srcFile);
//             out = new FileWriter(descFile);
            //3、读取和写入操作
            char[] buffer = new char[1000];//创建一个容量为 10 的字符数组，存储已经读取的数据
            int len = -1;//表示已经读取了多少个字节，如果是 -1，表示已经读取到文件的末尾
            while((len=br.read(buffer))!=-1){
                bw.write(buffer, 0, len);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            //4、关闭流资源
            try {
                br.close();
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
