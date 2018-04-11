package cn.itcast.bigdata.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份，-DHADOOP_USER_NAME=hadoop
 *也可以hdfs在通过构造时候传参进去
 *
 * @author LiaoYangJun
 * @createTime 2018/4/11.
 */
public class HdfsClientDemo {

    private FileSystem fs = null;

    @Before
    public void init() throws Exception{
        Configuration conf = new Configuration();

        //conf.set("fs.defaultFs", "hdfs://192.168.248.129");
        fs = FileSystem.get(new URI("hdfs://192.168.248.129:9000"), conf, "root");
    }

    /**
     * 下载
     * @throws IOException
     */
    @Test
    public void testDownLoad() throws IOException {
        fs.copyToLocalFile(new Path("hdfs"), new Path("bendi"));

    }

    /**
     * 上传
     * @throws IOException
     */
    @Test
    public void testUpload() throws IOException {
        fs.copyFromLocalFile(new Path("bendi"), new Path("hdfs"));
        fs.close();
    }

}
