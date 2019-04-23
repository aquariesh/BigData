package Flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author wangjx
 * @date 2019/4/11 18:27
 */
public class JavaTableSQLAPI {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);
        String filePath = "";
        DataSet<Sales> csv = env.readCsvFile(filePath)
                .ignoreFirstLine()
                .pojoType(Sales.class,"transactionId","customerId","itemId","amountPaid");
        //csv.print();
        Table sales = tableEnv.fromDataSet(csv);
        tableEnv.registerTable("sales",sales);
        Table resultTable = tableEnv.sqlQuery("");
        DataSet<Row> result = tableEnv.toDataSet(resultTable,Row.class);
    }

    public static class Sales{
        public String transactionId;
        public String customerId;
        public String itemId;
        public Double amountPaid;

    }
}
