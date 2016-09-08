package com.udbac.hadoop.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

/**
 * Created by 43890 on 2016/9/7.
 */
public class DefinedPartition extends Partitioner<DefinedKey, Text> {
    private static final Logger logger = Logger.getLogger(DefinedKey.class);

    public int getPartition(DefinedKey key, Text text, int numPartitions) {
        logger.debug("--------进入自定义partition--------");
        return (key.getDeviceId().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}