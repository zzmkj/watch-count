package com.geek.watch.top;

import com.geek.watch.utils.DateUtils;
import com.geek.watch.video.VideoInfoJob;
import com.geek.watch.video.VideoInfoMapper;
import com.geek.watch.video.VideoInfoReducer;
import com.geek.watch.video.VideoInfoWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Classname: VideoInfoTopJob
 * @Author: Ming
 * @Date: 2020/1/27 7:49 下午
 * @Version: 1.0
 * @Description: 统计每天开播时长最长的前10名主播及对应的开播时长
 **/
public class VideoInfoTopJob {
    public static void main(String[] args) throws Exception {
        /*if (args.length != 2) {
            System.exit(100);
        }*/
        Configuration configuration = new Configuration();

        //从输入路径中获取日期
        String[] splits = args[0].split("/");
        String dateTime = splits[splits.length - 1];
        String dt = DateUtils.transDateFormat(dateTime);
        configuration.set("dt", dt);

        FileSystem fs = FileSystem.get(configuration);
//        Path outputPath = new Path("video/top");
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(configuration);
        job.setJarByClass(VideoInfoTopJob.class);

        job.setMapperClass(VideoInfoTopMapper.class);
        job.setReducerClass(VideoInfoTopReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        FileInputFormat.setInputPaths(job, new Path("video/etl"));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println("result : " + result);
    }
}
