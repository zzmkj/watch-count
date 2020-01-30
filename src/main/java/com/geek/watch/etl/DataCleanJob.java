package com.geek.watch.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Classname: DataClean
 * @Author: Ming
 * @Date: 2020/1/26 2:11 下午
 * @Version: 1.0
 * @Description: 数据清洗作业
 * 1、从原始数据（JSON格式）中过滤出来需要的字段:
 * 主播id(uid)、金币数量(gold)、总观看PV(watchnumpv)、粉丝关注量(follower)、视频总开播时长(length)
 * 2、针对以上需要过滤出来的字段进行异常值判断：
 * 如果这些字段值为负值，则认为是异常数据，直接丢弃；如果这些字段值个别缺失，则认为字段的值为0即可。
 **/
public class DataCleanJob {
    public static void main(String[] args) throws Exception {
//        if (args.length != 2) {
//            System.exit(100);
//        }
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(DataCleanJob.class);

        job.setMapperClass(DataCleanMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //禁用reduce
        job.setNumReduceTasks(0);

//        FileInputFormat.setInputPaths(job, new Path("video/input/video.log"));
//        Path outputPath = new Path("video/etl");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println("result: " + result);
    }
}
