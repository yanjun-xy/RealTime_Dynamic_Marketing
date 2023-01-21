import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhouyanjun
 * @create 2023-01-18 17:29
 */
public class _04_WordCount_LambdaTest {
    public static void main(String[] args) throws Exception {
        // 创建一个编程入口（执行环境）

        // 流式处理入口环境
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = envStream.readTextFile("flink_course/data/wc/input/wc.txt");

        // 先把句子变大写

        /* 从map算子接收的是MapFunction接口实现对象，它是一个单抽象方法的接口。
        所以这个接口的实现类的核心功能，就在它的方法上，实现逻辑。
        写个类这里显得有些冗余，类里面既可以写逻辑还可以加 成员变量、构造方法。lambda是绝对做不到的。
        但是这里用不上，这里就是给个x，返回y的计算过程；
        那就用过程式的编程语言来写会更简洁，面向过程，也叫函数式编程。
        那就可以用lambda表达式来简洁实现;
        lambda表达式本质：本质上就是单方法接口的方法实现的简洁语法表达。
        如果接口中实现对象有多个方法要写，那lambda表达式写不了！
        streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return null;
            }
        });*/


        /**
         * lambda表达式怎么写，看你要实现的那个接口的方法接收什么参数，返回什么结果
         */
        // 然后就按lambda语法来表达：  (参数1,参数2,...) -> { 函数体 }  函数体对应特定方法的方法体
        //参数名可以随意
        // streamSource.map( (value) -> { return  value.toUpperCase();});

        // 由于上面的lambda表达式，参数列表只有一个，且函数体只有一行代码，则可以简化
        // streamSource.map( value ->  value.toUpperCase() ) ;

        // 由于上面的lambda表达式， 函数体只有一行代码，且其中的方法调用没有参数传递，参数只使用了一次，
        // 可以把函数调用转成  “方法引用”
        SingleOutputStreamOperator<String> upperCased = streamSource.map(String::toUpperCase);

        // 然后切成单词，并转成（单词,1），并压平
        /*upperCased.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        });*/
        // 从上面的接口来看，它依然是一个   单抽象方法的 接口，所以它的方法实现，依然可以用lambda表达式来实现
        //lambda表达式中，泛型问题非常重要，还不如写个接口的匿名实现类

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne =

                //如果传入参数类型没有添加泛型，collector这个输出参数会丢失泛型，这样输出的结果流因为不知道具体数据类型，
                //所以输出的数据类型是object类型；所以下游再去处理的时候数据流类型永远是object类型。这非常不方便。

                //所以， collector.collect()这个方法就不知道要收集的数据是Tuple2类型还是Object类型
                upperCased.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
                    //Tuple2自己也有泛型，有两个泛型参数
                    //编译结束之后的.class文件中，泛型的类型会被擦除，泛型的信息丢失了。只知道是Tuple2，并不知道Tuple2里面是什么东西。这是java的问题，并不是flink的问题
                    //java中泛型的功能，只是在编译的时候做类型的约束，类型的检查。所以编译时类型对应不上是会报错的。

                    String[] words = s.split("\\s+");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1));
                        //内部功能有返回数据，只是这个lambda语法没有返回
                    }
                })
                        //flink搞了一个机制，显式使用api来指定自己声明的数据类型。
                        //提供了好几个api
                        //.class是已经编译结束生成的东西。但是里面早就不存在泛型信息了。

                        // .returns(new TypeHint<Tuple2<String, Integer>>() {});// 通过 TypeHint 传达返回数据类型。是一个标记接口，没有任何抽象方法要去实现
//                         .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));  // 更通用的，是传入TypeInformation,上面的TypeHint也是封装了TypeInformation
                        .returns(Types.TUPLE(Types.STRING, Types.INT));  // 利用工具类Types的各种静态方法，来生成TypeInformation


        // 按单词分组
        //keyBy算子知道从上游过来wordAndOne流的数据类型是Tuple2，拿谁做key，那是开发人员的事情
        /*wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return null;
            }
        })*/
        // 从上面的KeySelector接口来看，它依然是一个 单抽象方法的 接口，所以它的方法实现，依然可以用lambda表达式来实现
        //接受参数value，返回value.f0
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy((value) -> value.f0);


        // 统计单词个数
        //sum并不需要传入什么接口
        keyedStream.sum(1)
                .print();


        envStream.execute();
    }
}
