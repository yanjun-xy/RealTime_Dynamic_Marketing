package cn.doitedu.rtmk.tech_test.bitmap_inject;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author zhouyanjun
 * @create 2023-01-18 16:15
 * @Desc: 从socket读取用户的实时的行为事件
 *        并从外部（规则管理平台的mysql元数据库）注入规则及规则对应的人群bitmap，
 *        对输入的Guid用户行为事件进行规则处理。
 *        验证：bitmap能不能用。对画像人群处理，对应Guid在不在画像人群当中，不在就不处理，在就处理。
 */
@Slf4j
public class _04_FlinkInjectRuleBimtapProcessEvents {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/checkpoint/");

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 获取  用户实时行为事件流  —— 主流
        DataStreamSource<String> eventStr = env.socketTextStream("doitedu", 4444);

        //map映射为一个元组(guid用户,一个事件)
        //用户行为事件流
        SingleOutputStreamOperator<Tuple2<Integer, String>> events =

                eventStr.map(new MapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> map(String line) throws Exception {
                String[] split = line.split(",");
                return Tuple2.of(Integer.parseInt(split[0]), split[1]); //guid字符串转换成整数
            }
        });






        // 2 获取规则系统的规则定义流
        // 创建cdc连接器表，去读mysql中的规则定义表的binlog
        tenv.executeSql("CREATE TABLE rtmk_rule_define (   " +
                "      rule_id STRING  PRIMARY KEY NOT ENFORCED,    " +
                "      profile_users_bitmap BINARY                  " +
                "     ) WITH (                                      " +
                "     'connector' = 'mysql-cdc',         " +
                "     'hostname' = 'doitedu'   ,         " +
                "     'port' = '3306'          ,         " +
                "     'username' = 'root'      ,         " +
                "     'password' = 'root'      ,         " +
                "     'database-name' = 'rtmk',          " +
                "     'table-name' = 'rtmk_rule_def'     " +
                ")");

        Table table = tenv.sqlQuery("select rule_id,profile_users_bitmap from rtmk_rule_define");

        //cdc连接器表转换为dataStream
        DataStream<Row> rowDataStream = tenv.toChangelogStream(table);


        //得到规则定义流
        DataStream<Tuple2<Tuple2<String,String>, RoaringBitmap >> ruleDefineStream =

                //Tuple2元组装两个东西，ruleId和bitmap
                //封装一下，(row,  ( (规则id，操作类型),bitmap) )
                rowDataStream.map(new MapFunction<Row, Tuple2<Tuple2<String,String>, RoaringBitmap >>() {
                    @Override
                    //从row里获取数据，把数据全部取出来
                    public Tuple2<Tuple2<String,String>, RoaringBitmap > map(Row row) throws Exception {

                        //获取rowid，获取bitmap
                        String rule_id = row.getFieldAs("rule_id");
                        byte[] bitmapBytes = row.getFieldAs("profile_users_bitmap");

                        // 反序列化本次拿到的规则的bitmap
                        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
                        bitmap.deserialize(ByteBuffer.wrap(bitmapBytes));

                        // 返回元组 (ruleId，对应的bitmap)
                        //添加下数据元素：规则id+数据流的操作类型+bitmap
                        return Tuple2.of(
                                //如果状态标记：规则id 更新、插入，那就put替换覆盖。如果删除就remove删除。
                                //只要不是delete，就是插入
                                Tuple2.of(rule_id,row.getKind() == RowKind.DELETE?"delete":"upsert") //给下游传递信息，插入更新都是一个动作upsert
                                , bitmap
                        );
                    }
                });









        // 4 将规则定义流，广播
        //广播状态。 广播流里拿到数据，要和主流共享数据。所以要定义一个合理的数据结构。所以泛型里就是id，bitmap
        MapStateDescriptor<String, RoaringBitmap> broadcastStateDesc =
                new MapStateDescriptor<>("rule_info", String.class, RoaringBitmap.class);

        //tuple2里面包含一个tuple2
        BroadcastStream<Tuple2<Tuple2<String, String>, RoaringBitmap>> broadcastStream = //广播之后，就得到了广播流，叫broadcastStream
                //保留的数据结构：规则id，bitmap
                ruleDefineStream.broadcast(broadcastStateDesc); //里面是广播状态，用来存储数据的。





        // 3 将 广播流  与 用户行为事件流，进行连接。-----> 这样 用户行为事件流 就可以拿到广播流（规则定义）里面的事件数据了。
        //拿到两个流，做一些事件的加工
        events
                .keyBy(tp->tp.f0) //不同的人发到不同的并行度。按照元组第一个guid来分
                .connect(broadcastStream)

                //使用process，主流、广播流的数据都能获取到了，然后可以操作了
                //输入第一个流（人，事件）；第二个流（规则id，bitmap），输入个string
                .process(new KeyedBroadcastProcessFunction<
                        Integer
                        , Tuple2<Integer, String>
                        , Tuple2<Tuple2<String, String>, RoaringBitmap>
                        , String>() {

                    /**
                     * 处理主流的方法。
                     * 我们获取到要 广播流-规则信息流的数据，把数据共享给主流的处理逻辑。让规则数据，可以被我的事件处理逻辑所使用。
                     */
                    @Override
                    public void processElement(Tuple2<Integer, String> event, KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Tuple2<String, String>, RoaringBitmap>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //拿到状态的描述
                        ReadOnlyBroadcastState<String, RoaringBitmap> broadcastState = ctx.getBroadcastState(broadcastStateDesc);

                        // 有很多规则，首先遍历。对系统内所持有的的每个规则，进行一次人群画像是否包含的判断
                        //一个entry里面就有规则id、bitmap
                        for (Map.Entry<String, RoaringBitmap> entry : broadcastState.immutableEntries()) {
                            String ruleId = entry.getKey(); //获取规则id
                            RoaringBitmap bitmap = entry.getValue(); //获取对应规则id的bitmap

                            out.collect(String.format("当前行为事件的用户：%d , 规则：%s 的目标人群是否包含此人： %s" ,event.f0,ruleId,
                                    bitmap.contains(event.f0)));
                        }
                    }


                    /**
                     * 处理广播流：规则信息流的方法。 mysql发送一条新加的规则数据到这个广播流中。
                     * Tuple2<Tuple2<String, String>, RoaringBitmap>
                     *
                     * Tuple2<Tuple2<规则id,操作类型>,人群bitmap>
                     * 广播状态，是两个流都可以共享的。
                     * 广播流里加工、处理新来到的数据。
                     */
                    @Override
                    public void processBroadcastElement(Tuple2<Tuple2<String, String>, RoaringBitmap> ruleInfo, KeyedBroadcastProcessFunction<Integer, Tuple2<Integer, String>, Tuple2<Tuple2<String, String>, RoaringBitmap>, String>.Context ctx, Collector<String> out) throws Exception {

                        //拿到（广播） 状态的描述
                        BroadcastState<String, RoaringBitmap> broadcastState = ctx.getBroadcastState(broadcastStateDesc);

                        //拿到了规则id，以及数据的状态情况。
                        //根据flinkcdc传入数据的标记信息，决定是存储进入广播状态，还是不存储进入广播状态

                        if(ruleInfo.f0.f1.equals("upsert")) {
                            //生产环境没有人通过sysout来打印输出，都是通过日志。这样可以通过日志的设置决定什么级别的日志可以输出
                            log.error("接收到一个新增的规则定义信息，独有的规则id为： {} ",ruleInfo.f0.f0); //定义这个级别，是为了输出醒目

                            //只要mysql增加新的规则，那广播流就 不断的收到新数据，就可以存放到 广播状态里面。
                            //存入新规则
                            broadcastState.put(ruleInfo.f0.f0, ruleInfo.f1);
                        }else{
                            log.error("根据规则管理平台的要求，删除了一个规则： {} ",ruleInfo.f0.f0);
                            broadcastState.remove(ruleInfo.f0.f0);
                        }
                    }
                }).print();

        env.execute();


    }
}
