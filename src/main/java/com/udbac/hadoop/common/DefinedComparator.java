package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 */
public class DefinedComparator extends WritableComparator {
    private static final Logger logger = Logger.getLogger(DefinedKey.class);

    public DefinedComparator() {
        super(DefinedKey.class, true);
    }

    public int compare(WritableComparable combinationKeyOne,
                       WritableComparable CombinationKeyOther) {
        logger.debug("---------进入自定义排序---------");

        DefinedKey c1 = (DefinedKey) combinationKeyOne;
        DefinedKey c2 = (DefinedKey) CombinationKeyOther;

        if (!c1.getDeviceId().equals(c2.getDeviceId())) {
            logger.debug("---------退出自定义排序1---------");
            return c1.getDeviceId().compareTo(c2.getDeviceId());
        } else {
            logger.debug("---------退出自定义排序2---------");
            return Long.compare(c1.getTimeStr(), c2.getTimeStr());
        }

    }
}
