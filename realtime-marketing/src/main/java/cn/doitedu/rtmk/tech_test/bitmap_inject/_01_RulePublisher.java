package cn.doitedu.rtmk.tech_test.bitmap_inject;

import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author zhouyanjun
 * @create 2023-01-18 11:00
 */
public class _01_RulePublisher {
    public static void main(String[] args) throws IOException, SQLException {
        //定义规则ID
//        String ruleId = "g01_rule01";
        //String ruleId = "g01_rule02";
        //String ruleId = "g01_rule03";
        String ruleId = "g01_rule04";


        // 根据规则的条件，去es中查询人群，得到目标人群
        //int[] ruleProfileUsers = {1,3,5,7,101,201}; //这里面就是guid，这些人满足这个规则
//        int[] ruleProfileUsers = {55, 3, 5, 7};
        int[] ruleProfileUsers = {100};

        // 把查询出来的人群的guid列表，变成bitmap
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(ruleProfileUsers);

        // 把生成好的bitmap，变成2进制，进行序列化，序列到一个字节数组中
        // 流，本质就是字节数组
        ByteArrayOutputStream bout = new ByteArrayOutputStream(); //字节数组流
        //流有个包装设计模式
        DataOutputStream dout = new DataOutputStream(bout);

        //serialize()里面要的是DataOutputStream
        bitmap.serialize(dout);//此时对象里面的数据就变成了二进制的线性的序列，往dout这个流里面插入，所有的数据都在这个流里面了

        //转换成字节数组，这样就拿到了bitmap的字节数组
        byte[] bitmapBytes = bout.toByteArray();

        // 将这个bitmap连同本规则的其他信息，一同发布到规则平台的元数据库中mysql
        // 下面就是写jdbc的代码，越是死代码，越没有技术含量
        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");
        PreparedStatement statement = conn.prepareStatement("insert into rtmk_rule_def values(?,?)");
        statement.setString(1, ruleId);
        //setBytes()
        statement.setBytes(2, bitmapBytes);

        statement.execute();

        statement.close(); //语句执行器关闭
        conn.close(); //连接器关闭

    }

}
