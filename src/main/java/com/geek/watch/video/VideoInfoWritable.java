package com.geek.watch.video;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Classname: VideoInfoWritable
 * @Author: Ming
 * @Date: 2020/1/27 2:42 下午
 * @Version: 1.0
 * @Description: 直播统计信息封装
 **/
public class VideoInfoWritable implements Writable {

    private long gold;
    private long watchnumpv;
    private long follower;
    private long length;

    public VideoInfoWritable() {

    }

    public VideoInfoWritable(long gold, long watchnumpv, long follower, long length) {
        this.gold = gold;
        this.watchnumpv = watchnumpv;
        this.follower = follower;
        this.length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(gold);
        out.writeLong(watchnumpv);
        out.writeLong(follower);
        out.writeLong(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.gold = in.readLong();
        this.watchnumpv = in.readLong();
        this.follower = in.readLong();
        this.length = in.readLong();
    }

    @Override
    public String toString() {
        return gold + "\t" + watchnumpv + "\t" + follower + "\t" + length;
    }

    public long getGold() {
        return gold;
    }

    public void setGold(long gold) {
        this.gold = gold;
    }

    public long getWatchnumpv() {
        return watchnumpv;
    }

    public void setWatchnumpv(long watchnumpv) {
        this.watchnumpv = watchnumpv;
    }

    public long getFollower() {
        return follower;
    }

    public void setFollower(long follower) {
        this.follower = follower;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
