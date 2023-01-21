import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author zhouyanjun
 * @create 2023-01-18 22:11
 */
public class _16_BroadCast_Demo {
    public static void main(String[] args) throws Exception {

//        Configuration configuration = new Configuration();
//        configuration.setInteger("rest.port", 8822);
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // id,eventId   一个Guid用户，做了什么事件，是可能做很多事件的。持续不断的事件流。
        DataStreamSource<String> stream1 = env.socketTextStream("192.168.77.88", 9998);

        SingleOutputStreamOperator<Tuple2<String, String>> s1 = stream1.map(s -> {
            String[] arr = s.split(",");
            return Tuple2.of(arr[0], arr[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {
        });

        // id,age,city。 不是持续不断的流。
        DataStreamSource<String> stream2 = env.socketTextStream("192.168.77.88", 9999);

        //两个流的join，在生产中，被称作把 “把数据打宽”

        SingleOutputStreamOperator<Tuple3<String, String, String>> s2 = stream2.map(s -> {
            String[] arr = s.split(",");
            return Tuple3.of(arr[0], arr[1], arr[2]);
        }).returns(new TypeHint<Tuple3<String, String, String>>() {
        });


        /**
         * 案例的业务背景：
         *    流 1：  用户行为事件流（持续不断，同一个人也会反复出现，出现次数不定
         *    流 2：  用户维度信息（年龄，城市），同一个人的数据只会来一次，来的时间也不定 （作为广播流）
         *    需要加工流1，把用户的维度信息填充好，利用广播流来实现
         */

        // 将字典数据所在流： s2   ====>  转成 广播流.  本质就是广播流里的数据会存入在状态中。



        MapStateDescriptor<String, Tuple2<String, String>> userInfoStateDesc =
                new MapStateDescriptor<>(
                        "userInfoStateDesc"
                        , TypeInformation.of(String.class) //把id作为key，这样可以和另一条流进行关联。应付java泛型擦除的情况，输入泛型的类型
                        , TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
                })
                );


        BroadcastStream<Tuple3<String, String, String>> s2BroadcastStream =
                //本质上就是把s2这个流的数据存放在 另外一条流的状态里面去。状态的存储结构是hashmap结构（有k和v）
                //现在就是要描述好这个状态存储数据的数据结构，以及 key 、value的数据类型
                s2.broadcast(userInfoStateDesc);


        // 哪个流处理中需要用到广播状态中的数据，就要 去  连接 connect  这个广播流。
        // 这样就保证了两个流之间是可以发生关联关系
        BroadcastConnectedStream<Tuple2<String, String>, Tuple3<String, String, String>> connected =
                s1.connect(s2BroadcastStream); // connect连接，就是让两个流拥有共同的状态，实现状态的共享


        /**
         *   对 连接了广播流之后的 “连接流” 进行处理
         *   核心思想：
         *      在processBroadcastElement方法中，把获取到的广播流中的数据，插入到 “广播状态”中
         *      在processElement方法中，对取到的主流数据进行处理（从广播状态中获取要拼接的数据，拼接后输出）
         */

        SingleOutputStreamOperator<String> resultStream =
                connected.process(
                        new BroadcastProcessFunction<
                                Tuple2<String, String> //左边流
                                , Tuple3<String, String, String>//右边流
                                , String>//输出类型
                                () {

                            //这个函数接口，有两个方法要去实现
                            /*BroadcastState<String, Tuple2<String, String>> broadcastState;*/

                            //connect之后的广播流，每个流都保留着各自的独立性。
                            //和connect算子方法会返回connectedStream一样，每个流都有各自的独立性。

                            //基础逻辑：两个方法要公用一套数据，在两个方法之间进行数据的沟通。那就可以通过一个共同的成员来实现数据的共享。
                            //但是在flink中不能使用自己定义的成员变量，因为如果程序崩溃了，然后重启，那么这个变量里面保留的东西就没有了。
                            //所以我们要使用flink自带的状态机state来保存数据，会持久化到磁盘，保证数据不丢，来保证数据的容错性！！！


                            /**
                             * 本方法，是用来处理 主流中的数据（每来一条，调用一次）
                             * 就3个参数public void processElement(element,ctx,out){...}，意义分别：
                             * @param element  左流（主流）中的一条数据
                             * @param ctx  上下文。只有运行时的各种环境信息，不会存任何数据。
                             * @param out  输出器
                             * @throws Exception
                             */
                            @Override
                            public void processElement(Tuple2<String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                //目前语法上没有出错，那么在运行时有没有可能出错？ 如果两个流的数据过来有先后顺序，那可能报空指针异常。

                                // 通过 ReadOnlyContext ctx 见名知意。访问获取广播状态对象中的数据，限制：这是一个 “只读 ” 的对象！
                                //这是保证，避免如果对状态做了修改的操作，就有可能造成不可预料的问题。
                                //主流数据，有多个算子，每个算子有多个并行度，每个人（算子）都持有广播流的状态数据，每个人（算子）都可以去访问这份状态数据。
                                //以只读的形式去访问这份广播状态数据，是安全的。如果某个算子修改了广播状态数据，其他的并行度的算子就没法同步
                                //这就会导致并行计算的算子中，这份广播状态数据是不一样的。没法同步数据。
                                //要保证数据的一致性！！！


                                ReadOnlyBroadcastState<String, Tuple2<String, String>> broadcastState =
                                        ctx.getBroadcastState(userInfoStateDesc); //
                                // 广播流新传输一个新数据，存入状态。主流从这个状态中获取到。这个状态保证了主流和广播流的数据是相通的。


                                if (broadcastState != null) { //状态不为空，也就是代表有数据
                                    Tuple2<String, String> userInfo = broadcastState.get(element.f0); //获取数据。但是根据key获取value
                                    // ，也有可能是null空值。所以在后面还是要进行判断。

                                    //下面是进行一个字符串的拼接
                                    out.collect(element.f0 + ","
                                            + element.f1 + ","
                                            //三元表达式，==null吗，是的那就返回null，否则返回userInfo.f0。用括号括起来，保证运算优先级。
                                            + (userInfo == null ? null : userInfo.f0) + ","
                                            + (userInfo == null ? null : userInfo.f1));
                                } else { //没有数据，那就输出null
                                    out.collect(element.f0 + "," + element.f1 + "," + null + "," + null);
                                }

                            }

                            /**
                             * 本方法：处理广播流中的数据
                             * 就3个参数：public void processBroadcastElement(element,ctx,out){...}
                             * @param element 广播流中的一条数据
                             * @param ctx  上下文。见名知意，是可以进行修改的操作。。
                             * @param out 输出器
                             * @throws Exception
                             */
                            @Override
                            public void processBroadcastElement(Tuple3<String, String, String> element, BroadcastProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, String>.Context ctx, Collector<String> out) throws Exception {
                                // 从上下文中，获取广播状态对象（可读可写的状态对象）
                                BroadcastState<String, Tuple2<String, String>> broadcastState =
                                        ctx.getBroadcastState(userInfoStateDesc); //传入的参数是MapStateDescriptor广播状态，本质就是hashmap 只不过是被flink所管理着的hashmap，不是以普通的hashmap。

                                // 然后将获得的这条  广播流数据， 拆分后，存储 到这个广播状态(变量)中-------------------->下一步：然后主流 从这个广播状态变量中获取这个新存储到的数据
                                broadcastState.put(element.f0, Tuple2.of(element.f1, element.f2));
                                //状态是一个一直都在的东西，存储进来后就会一直进行保留！！！

                            }
                        });


        resultStream.print();


        env.execute();

    }
}
