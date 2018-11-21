package utils;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;

/**
 * hbase工具类
 */
public class HbaseUtils {
    Configuration configuration = null;
    Admin admin = null;
    /**
     * 私有构造方法
     */
    private HbaseUtils(){
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost:2181");
        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");

        try {
            admin = ConnectionFactory.createConnection(configuration).getAdmin();
            admin = ConnectionFactory.createConnection(configuration).getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 单例模式获取hbase链接
     */
    private static HbaseUtils instance = null;
    public static synchronized HbaseUtils getInstance(){
        if(null == instance){
            instance = new HbaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取到HTable实例
     */
    public HTable getTable(String tableName){
        HTable table = null ;
        try {
            table = new HTable(configuration,tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     *
     * @param tableName hbase表名
     * @param rowkey    hbase表的rowkey
     * @param cf    hbase表的columnfamily列族
     * @param column    hbase表的具体列
     * @param value     写入hbase表的值
     */
    public void put(String tableName,String rowkey,String cf,String column,String value){
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
