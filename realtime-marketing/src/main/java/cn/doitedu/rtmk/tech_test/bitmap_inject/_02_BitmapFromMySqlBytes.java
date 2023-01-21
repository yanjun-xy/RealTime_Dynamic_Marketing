package cn.doitedu.rtmk.tech_test.bitmap_inject;

import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;

/**
 * @author zhouyanjun
 * @create 2023-01-18 11:00
 */
public class _02_BitmapFromMySqlBytes {
    public static void main(String[] args) throws SQLException, IOException {
        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:3306/rtmk", "root", "root");

        // 先从mysql中获得规则Id：g01_rule01；根据规则id就可以获取对应的画像人群
        PreparedStatement statement = conn.prepareStatement("select rule_id,profile_users_bitmap from rtmk_rule_def where rule_id = ? ");
        statement.setString(1,"g01_rule01");

        ResultSet resultSet = statement.executeQuery();
        resultSet.next();
        byte[] bitmapBytes = resultSet.getBytes("profile_users_bitmap");

        statement.close();
        conn.close();

        //获取字节数组之后是不能查看的，需要反序列化bitmap字节数组
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf();
        bitmap.deserialize(ByteBuffer.wrap(bitmapBytes)); //反序列化。这是不需要弄个流，因为byte长度确定

        // 测试，反序列化出来bitmap是否是之前序列化的数据
        // 查看序列化之前的bitmap中是否包含guid：55, 3, 5, 7
        System.out.println(bitmap.contains(55));
        System.out.println(bitmap.contains(3));
        System.out.println(bitmap.contains(5));
        System.out.println(bitmap.contains(5));
        System.out.println(bitmap.contains(88));
//        true
//        true
//        true
//        true
//        false
    }

}
