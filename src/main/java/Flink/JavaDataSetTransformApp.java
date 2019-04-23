package Flink;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangjx
 * @date 2019/4/11 10:38
 */
public class JavaDataSetTransformApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //firstFunction(env);
        //flatMapFunction(env);
        joinFunction(env);
    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer,String>> info = new ArrayList<Tuple2<Integer, String>>();
        info.add(new Tuple2(1,"hadoop"));
        info.add(new Tuple2(1,"spark"));
        info.add(new Tuple2(1,"flink"));
        info.add(new Tuple2(2,"java"));
        info.add(new Tuple2(2,"spring boot"));
        info.add(new Tuple2(3,"linux"));
        info.add(new Tuple2(4,"wangjx"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);
        data.first(3).print();
        data.groupBy(0).first(2).print();
        data.groupBy(0).sortGroup(1,Order.ASCENDING).first(2).print();
    }

    public static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data = env.fromCollection(info);

    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer,String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1,"猪蹄膀"));
        info1.add(new Tuple2(2,"spark"));
        info1.add(new Tuple2(3,"flink"));
        info1.add(new Tuple2(4,"java"));

        List<Tuple2<Integer,String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1,"北京"));
        info2.add(new Tuple2(2,"上海"));
        info2.add(new Tuple2(3,"成都"));
        info2.add(new Tuple2(5,"杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);
        
        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<Integer, String, String>(first.f0,first.f1,second.f1);
            }
        }).print();
    }
}
