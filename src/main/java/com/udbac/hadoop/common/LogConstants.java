package com.udbac.hadoop.common;

import org.apache.commons.lang.StringUtils;
import sun.swing.StringUIClientPropertyKey;

/**
 * Created by root on 2016/7/12.
 */
public class LogConstants {
    /**
     * 默认值
     */
    public static final String DEFAULT_VALUE = "unknown";
    /**
     * 一天的毫秒数
     */
    public static final int DAY_OF_MILLISECONDS = 86400000;
    /**
     * 半小时的毫秒数
     */
    public static final int HALFHOUR_OF_MILLISECONDS = 1800000;
    /**
     * 定义的运行日期
     */
    public static final String RUNNING_DATE_PARAMES = "RUNNING_DATE";
    /**
     * 日志分隔符
     */
    public static final String LOG_SEPARTIOR = " ";

    public enum UserDomain {
        OUTTAKE("m.4008823823.com.cn", "outtake"),
        SELFTAKE("order.kfc.com.cn", "selftake"),
        MEMBERTAKE("mall.kfc.com.cn", "membertake");

        public String domainName;
        public String alias;

        UserDomain(String domainName, String alias) {
            this.domainName = domainName;
            this.alias = alias;
        }

        public static String getAlias(String domainName) {
            for (UserDomain userDomain : values()) {
                domainName.trim();
                if (userDomain.getDomainName().equals(domainName)) {
                    return userDomain.getAlias();
                }
            }
            return DEFAULT_VALUE;
        }

        public String getDomainName() {
            return domainName;
        }

        public String getAlias() {
            return alias;
        }

    }

}
