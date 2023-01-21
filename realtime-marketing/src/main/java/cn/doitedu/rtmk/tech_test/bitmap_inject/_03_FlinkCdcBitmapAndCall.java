package cn.doitedu.rtmk.tech_test.bitmap_inject;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;

/**
 * @author zhouyanjun
 * @create 2023-01-18 11:19
 */
public class _03_FlinkCdcBitmapAndCall {
    public static void main(String[] args) throws Exception {
        //Flink编程套路，要有个环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的路径，测试，本地路径
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint/");

        //由于需要将Table转为DataStream，所以需要使用StreamTableEnvironment
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 创建cdc连接器表，去读mysql中的规则定义表的binlog
        // flink中创建一张表
        tenv.executeSql("CREATE TABLE rtmk_rule_define (   " +
                "      rule_id STRING  PRIMARY KEY NOT ENFORCED,    " +
                "      profile_users_bitmap BINARY                  " +  //字节数组在flinkSQL中类型
                "     ) WITH (                                      " +
                "     'connector' = 'mysql-cdc',         " +
                "     'hostname' = 'doitedu'   ,         " +
                "     'port' = '3306'          ,         " +
                "     'username' = 'root'      ,         " +
                "     'password' = 'root'      ,         " +
                "     'database-name' = 'rtmk',          " +
                "     'table-name' = 'rtmk_rule_def'     " +
                ")");

        //拿到一个表对象了，就可以转为流了
        Table table = tenv.sqlQuery("select rule_id,profile_users_bitmap from rtmk_rule_define");

        //将结果转换为DataStream数据流
        //数据库中的每一行数据，在flink表里就是一个row
        DataStream<Row> rowDataStream = tenv.toChangelogStream(table);


        //只是测试下bitmap能不能调用，输出个String
        rowDataStream.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
                //DataStream 和Table之间的转换
                //每个结果行都会带一个标记，可以使用 row.getKind()方法来获得，可查询的更改标志。
                RowKind kind = row.getKind();
                if (kind == RowKind.INSERT) { //
                    String rule_id = row.<String>getFieldAs("rule_id"); //点击源码，查看在哪里写泛型
                    byte[] bitmapBytes = row.<byte[]>getFieldAs("profile_users_bitmap"); //拿到bitmap的字节数组

                    // 反序列化 本次拿到的规则的bitmap
                    RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
                    bitmap.deserialize(ByteBuffer.wrap(bitmapBytes));

                    // 判断55,3,5,7等Guid用户是否在其中
                    boolean res55 = bitmap.contains(55);
                    boolean res3 = bitmap.contains(3);
                    boolean res77 = bitmap.contains(77);

                    out.collect(String.format("规则：%s, 用户：55, 存在于规则人群否: %s ", rule_id, res55));
                    out.collect(String.format("规则：%s, 用户：3, 存在于规则人群否: %s ", rule_id, res3));
                    out.collect(String.format("规则：%s, 用户：77 , 存在于规则人群否: %s ", rule_id, res77));
                }

            }
        }).print();


        env.execute();

    }
}
