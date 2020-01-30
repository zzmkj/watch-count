package com.geek.watch.top;

import com.geek.watch.utils.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Classname: VideoInfoTopReducer
 * @Author: Ming
 * @Date: 2020/1/27 7:53 下午
 * @Version: 1.0
 * @Description: TODO
 **/
public class VideoInfoTopReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    //保存主播id和直播时长
    private Map<String, Long> info = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long lengthSum = 0;
        for (LongWritable value : values) {
            lengthSum += value.get();
        }

        info.put(key.toString(), lengthSum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        //从配置类中取出dt参数(日期时间)
        String dt = conf.get("dt");

        //根据Map中的value排序
        Map<String, Long> sortedMap = MapUtils.sortValue(info);
        //初始值为1，循环10次，取出前10的主播id和开播时长写到输出文件中。
        AtomicInteger count = new AtomicInteger(1);
        sortedMap.forEach((key, val) -> this.writeResult(key, val, dt, context, count));
    }

    private void writeResult(String key, Long val, String dt, Context context, AtomicInteger count) {
        try {
            if (count.get() > 10) {
                return;
            }
            context.write(new Text(dt + "\t" + key), new LongWritable(val));
        } catch (Exception e) {
            e.printStackTrace();
        }
        count.getAndIncrement();
    }
}
