#!/bin/bash

# 如果需要加入定时任务crontab中，可以把下行命令复制粘贴到/etc/crontab中
# 30 00 * * * root /bin/bash /data/soft/jobs/etl.sh >> /data/soft/jobs/etl.log

#判断用户是否输入日期参数，如果没有输入则默认获取昨天日期
if [ "x$1" = "x" ]
then
  yes_time=`date + %Y%m%d --date="1 days ago"`
else
  yes_time=$1
fi


etljob_input=hdfs://hadoop000:8020/data/videoinfo/${yes_time}
etljob_output=hdfs://hadoop000:8020/data/videoinfo_etl/${yes_time}

videoinfo_job_input=${etljob_output}
videoinfo_job_output=hdfs://hadoop000:8020/res/videoinfojob/${yes_time}

videoinfo_top10_job_input=${etljob_output}
videoinfo_top10_job_output=hdfs://hadoop000:8020/res/top10/${yes_time}

jobs_home=/data/soft/jobs

# 删除输出目录 为了兼容脚本重跑的情况 【note：如果程序中已进行了这步兼容操作，可注释掉这行脚本】
# hdfs dfs -rm -r ${cleanjob_output}
# hdfs dfs -rm -r ${videoinfo_job_output}
# hdfs dfs -rm -r ${videoinfo_top10_job_output}

# 执行数据清洗任务【ETL操作】
hadoop jar \
${jobs_home}/watch-1.0-SNAPSHOT.jar \
com.geek.watch.etl.DataCleanJob \
${etljob_input} \
${etljob_output}

# 判断数据清洗任务(ETL)是否执行成功
hdfs dfs -ls ${etljob_output}/_SUCCESS
if [ "$?" = 0 ]
then
  echo "ETL job execute success"
  #执行指标统计任务1
  hadoop jar \
  ${jobs_home}/watch-1.0-SNAPSHOT.jar \
  com.geek.watch.video.VideoInfoJob \
  ${videoinfo_job_input} \
  ${videoinfo_job_output}

  # 判断统计任务1是否执行成功
  hdfs dfs -ls ${videoinfo_job_output}/_SUCCESS
  if [ "$?" != 0 ]
  then
    echo "VideoInfoJob execute faild ... date time is is ${yes_time}"
    # 执行失败，可以给管理员发送短信、邮箱
    # 具体可以调用接口或者脚本发送短信（Java、Python...）
  fi

  #执行指标统计任务2
  hadoop jar \
  ${jobs_home}/watch-1.0-SNAPSHOT.jar \
  com.geek.watch.top.VideoInfoTopJob \
  ${videoinfo_top10_job_input} \
  ${videoinfo_top10_job_output}

  # 判断统计任务2是否执行成功
  hdfs dfs -ls ${videoinfo_top10_job_output}/_SUCCESS
  if [ "$?" != 0 ]
  then
    echo "VideoInfoTopJob execute faild ... date time is is ${yes_time}"
    # 执行失败，可以给管理员发送短信、邮箱
    # 具体可以调用接口或者脚本发送短信（Java、Python...）
  fi

else
  echo "ETL job execute faild ... date time is is ${yes_time}"
  # 执行失败，可以给管理员发送短信、邮箱
  # 具体可以调用接口或者脚本发送短信（Java、Python...）
fi