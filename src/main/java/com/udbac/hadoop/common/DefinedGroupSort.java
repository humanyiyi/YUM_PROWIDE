package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 进行自定义分组 根据自定义key的 第一个field 即用户ID进行分组
 */
public class DefinedGroupSort extends WritableComparator {
    private static final Logger logger = Logger.getLogger(DefinedKey.class);

    public DefinedGroupSort() {
        super(DefinedKey.class, true);
    }

    @Override
    public int compare(WritableComparable combinationKeyOne, WritableComparable combinationKeyOther) {
        logger.debug("-------进入 自定义分组 flag-------");
        DefinedKey keyOne = (DefinedKey) combinationKeyOne;
        DefinedKey keyOther = (DefinedKey) combinationKeyOther;
        logger.debug("-------自定义分组key:" + keyOne.getDeviceId().
                compareTo(keyOther.getDeviceId()) + "-------");
        logger.debug("-------瑞出自定义分组 flag-------");
        return keyOne.getDeviceId().compareTo(keyOther.getDeviceId());
    }
}
