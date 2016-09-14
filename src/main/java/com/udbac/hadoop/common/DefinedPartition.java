package com.udbac.hadoop.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 进行自定义分区 根据自定义key的 第一个field 即用户ID进行分区
 */
public class DefinedPartition extends Partitioner<DefinedKey, Text> {
    private static final Logger logger = Logger.getLogger(DefinedKey.class);

    public int getPartition(DefinedKey key, Text text, int numPartitions) {
        logger.debug("--------进入自定义partition--------");
        //& 运算是为了保证hashcode后为正数
        return (key.getDeviceId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}