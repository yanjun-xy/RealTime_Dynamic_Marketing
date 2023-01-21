import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

/**
 * @author zhouyanjun
 * @create 2023-01-19 13:48
 */
public class _07_Transformation_Demos {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //这种类型的数据： {"uid":1,"name":"zs","friends":[{"fid":2,"name":"aa"},{"fid":3,"name":"bb"}]}
        //因为json数据本身是会带有引号的，所以在idea中，会对引号“”进行自动的转义


        /*DataStreamSource<String> streamSource = env.fromElements(
                "{\"uid\":1,\"name\":\"zs\",\"friends\":[{\"fid\":2,\"name\":\"aa\"},{\"fid\":3,\"name\":\"bb\"}]}",
                "{\"uid\":2,\"name\":\"ls\",\"friends\":[{\"fid\":1,\"name\":\"cc\"},{\"fid\":2,\"name\":\"aa\"}]}",
                "{\"uid\":3,\"name\":\"ww\",\"friends\":[{\"fid\":2,\"name\":\"aa\"}]}",
                "{\"uid\":4,\"name\":\"zl\",\"friends\":[{\"fid\":3,\"name\":\"bb\"}]}",
                "{\"uid\":5,\"name\":\"tq\",\"friends\":[{\"fid\":2,\"name\":\"aa\"},{\"fid\":3,\"name\":\"bb\"}]}"
        );*/


        DataStreamSource<String> streamSource = env.readTextFile("Flink_stydy/data_test/transformation_input/userinfo.txt");


        /**
         * map算子的演示
         */
        // 把每条json数据，转成javabean数据。
        // 那就必须要“json解析”，json和javabean是天生一对。任何一种数据结构的javabean都可以用json来进行表达。我用json表达的我也一定能用javabean表达。、
        //看上面的json数据结构，那就至少要有两个类，外层一类，里面一个类。

        SingleOutputStreamOperator<UserInfo> beanStream =
                streamSource.map(json -> JSON.parseObject(json, UserInfo.class)); //把json字符串转成一个类
//        beanStream.print();

        /**
         * filter算子的演示
         *   请过滤掉好友超过3位的用户数据
         */
        SingleOutputStreamOperator<UserInfo> filtered =
                beanStream.filter(bean -> bean.getFriends().size() <= 3);
//        filtered.print();


        /**
         * flatmap算子的演示
         *  把每个用户的好友信息，全部提取出来（带上用户自身的信息），并且压平，放入结果流中。
         *  压平：一个数据结果，变成新数据集中一行数据。
         *  一行变多行，在SQL里面是explode炸裂。炸裂之后还要和其他的行数据产生连接，在SQL里面是lateral view
         *  在hiveSQL中：
         *  select * from table_1 lateral view explode(数组字段 friends) table_2 as 字段名 —— 所以技术都是相通的
         *
         *
         *
         *  {"uid":1,"name":"zs","gender":"male","friends":[{"fid":2,"name":"aa"},{"fid":3,"name":"bb"}]}
         *  =>
         *  {"uid":1,"name":"zs","gender":"male","fid":2,"fname":"aa"}
         *  {"uid":1,"name":"zs","gender":"male","fid":3,"fname":"bb"}
         */


        SingleOutputStreamOperator<UserFriendInfo> flatten =
                //代码冗余，不代表运行时冗余。运行时是一样的效果。
                //自定义一个新的javabean，这样如果数据元素多的话，会更方便的操作bean

                //下面flatMap被称作datastream流上的算子
                filtered.flatMap(new FlatMapFunction<UserInfo, UserFriendInfo>() {
                    @Override
                    //下面的flatMap()是接口里面的普通方法。这里面就和flink没有任何关系了，就是java基础相关的操作
                    public void flatMap(UserInfo value, Collector<UserFriendInfo> out) throws Exception {

                        // 把friends列表提取出来，一个一个地返回
                        List<FriendInfo> friends = value.getFriends();
                /* lambda表达式写法：friends.forEach(x->out.collect(new UserFriendInfo(value.getUid(), value.getName(), value.getGender
                (),x
                .getFid(),x.getName() )));*/

                        //这个有一条数据，就变成3条数据了
                        for (FriendInfo x : friends) {
                            out.collect(new UserFriendInfo(value.getUid(), value.getName(), value.getGender(), x.getFid(), x.getName()));
                        }
                    }
                });

//        flatten.print();


        /**
         * keyBy算子的演示
         * 对上一步的结果，按用户性别分组
         *
         * 滚动聚合算子（只能在 KeyedStream 流上调用）：  sum算子 、 min算子 、 minBy算子 、 max算子、  maxBy算子、 reduce算子的演示
         * 并统计：
         *    各性别用户的好友总数
         *
         *    各性别中，用户好友数量最大值
         *    各性别中，用户好友数量最小值
         *
         *    求各性别中，好友数量最大的那个人
         *    求各性别中，好友数量最小的那个人
         *
         */
        // 各性别用户的好友总数
        KeyedStream<Tuple2<String, Integer>, String> keyedStream =
                flatten
                        .map(bean -> Tuple2.of(bean.getGender(), 1)) //返回一元组
                        .returns(new TypeHint<Tuple2<String, Integer>>() {}) //解决java中泛型擦除的问题
                        .keyBy(tp -> tp.f0) //按第一个角标分组
                ;

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumStream
                = keyedStream
                .sum(1);//通过字段求累加值。所以通过第二个角标来累加

//        sumStream.print();

        // 各性别中，用户好友数量最大值

        /**
         * max / maxBy  都是滚动聚合：  算子不会把收到的所有数据全部攒起来；而是只在状态中记录上一次的聚合值，然后当新数据到达的时候，会根据逻辑去更新 状态中记录的聚合值，并输出最新状态数据
         * max / maxBy  区别： 更新状态的逻辑！  max只更新要求最大值的字段；  而 maxBy 会更新整条数据；
         * 同理，min和minBy也如此
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> tuple4Stream =
                beanStream.map(
                        bean -> Tuple4.of(bean.getUid(), bean.getName(), bean.getGender(), bean.getFriends().size())
                        // max算子是通过字段来确定。所以根据json数据结构，先把数据加工成元组数据结构
                )
                        .returns(new TypeHint<Tuple4<Integer, String, String, Integer>>() {});


        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> genderUserFriendsMaxCount = tuple4Stream
                .keyBy(tp -> tp.f2)
                /*.max(3); */              //0123 各性别中，用户好友数量最大值
                .maxBy(3);// 求各性别中，好友数量最大的那个人

//        genderUserFriendsMaxCount.print();


        /**
         * reduce 算子 使用演示
         * 需求： 求各性别中，好友数量最大的那个人，而且如果前后两个人的好友数量相同，则输出的结果中，也需要将uid/name等信息更新成后面一条数据的值
         *
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduceResult =
                tuple4Stream.keyBy(tp -> tp.f2)
                        .reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
                            /**
                             *
                             * @param value1  是此前的聚合结果
                             * @param value2  是本次的新数据
                             * @return 返回值：更新后的最新聚合结果
                             * @throws Exception
                             */
                            @Override
                            public Tuple4<Integer, String, String, Integer> reduce(
                                    Tuple4<Integer, String, String, Integer> value1
                                    , Tuple4<Integer, String, String, Integer> value2
                            ) throws Exception {
                                //判断
                                if (value1 == null || value2.f3 >= value1.f3) {
                                    return value2; //更新、替换数据值
                                } else {
                                    return value1;
                                }
                            }
                        });
        /*reduceResult.print();*/


        /**
         * 用reduce，来实现sum算子的功能
         * 求：  对上面的  4元组数据  1,ua,male,2  ，求各性别的好友数总和
         * TODO
         */
        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> reduceSum =
                tuple4Stream.keyBy(tp -> tp.f2)
                        .reduce(new ReduceFunction<Tuple4<Integer, String, String, Integer>>() {
                            @Override
                            //参数中传递两条数据值，一条是old中间聚合值；一条是新输入的数据值
                            public Tuple4<Integer, String, String, Integer> reduce(Tuple4<Integer, String, String, Integer> value1,
                                                                                   Tuple4<Integer, String, String, Integer> value2
                            ) throws Exception
                            {
                                //万一值是空，null肯定不能做加法，这样就会报空指针异常。
                                //改的是第3个元素
                                //set方法没有返回值
                                value2.setField(value2.f3 + (value1 == null ? 0 : value1.f3), 3);
                                return value2;
                            }
                        });

//        reduceSum.print();


        env.execute();
    }
}

@Data
class FriendInfo implements Serializable { //实现序列化，因为数据可能在各个节点之间传来传去。不然单机测试没问题，但是集群运行会有问题
    private int fid;
    private String name;
}


@Data
class UserInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private List<FriendInfo> friends; //json数据中有个数组，那这里用list来装对象
}


@Data
@AllArgsConstructor
        //有参构造
class UserFriendInfo implements Serializable {
    private int uid;
    private String name;
    private String gender;
    private int fid;
    private String fname;

}