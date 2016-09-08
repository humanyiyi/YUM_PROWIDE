package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

/**
 * Created by root on 2016/7/25.
 */
public class DefinedGroupSort extends WritableComparator {
    private static final Logger logger = Logger.getLogger(DefinedKey.class);

    public DefinedGroupSort() {
        super(DefinedKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        logger.debug("-------进入 自定义分组 flag-------");
        DefinedKey ck1 = (DefinedKey) a;
        DefinedKey ck2 = (DefinedKey) b;
        logger.debug("-------自定义分组key:" + ck1.getDeviceId().
                compareTo(ck2.getDeviceId()) + "-------");
        logger.debug("-------瑞出自定义分组 flag-------");
        return ck1.getDeviceId().compareTo(ck2.getDeviceId());
    }
}
