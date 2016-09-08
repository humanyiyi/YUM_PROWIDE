package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.DefinedKey;
import com.udbac.hadoop.common.LogConstants;
import com.udbac.hadoop.entity.WideTable;
import com.udbac.hadoop.etl.util.IPSeekerExt;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 */
public class LogAnalyserReducer extends Reducer<DefinedKey, Text, NullWritable, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserReducer.class);
    private static IPSeekerExt ipSeekerExt = new IPSeekerExt();

    @Override
    protected void reduce(DefinedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        try {
            long cur;
            long tmp;
            long last = 0;
            BigDecimal duration = null;
            WideTable wideTable = null;
            Map<String, WideTable> oneVisit = new HashMap<>();
            for (Text text : values) {
                String log = text.toString();
                String[] logSplits = log.split("\\|");
                cur = TimeUtil.timeToLong(logSplits[2]);
                if (cur - last < LogConstants.HALFHOUR_OF_MILLISECONDS) {
                    tmp = cur - last;
                    duration = duration.add(BigDecimal.valueOf(tmp));
                    wideTable.setDuration(duration);
                } else {
                    duration = BigDecimal.ZERO;
                    wideTable = WideTable.parse(log);
                    handleTab(wideTable);
                    oneVisit.put(UUID.randomUUID().toString().replace("-", ""), wideTable);
                }
                last = cur;
            }
            for (Map.Entry<String, WideTable> entry : oneVisit.entrySet()) {
                context.write(NullWritable.get(), new Text(entry.getKey() + "|" + entry.getValue()));
            }
        } catch (Exception e) {
            logger.error("session重建出现异常 deviceId:" + key.getDeviceId() + e);
        }
    }

    private static void handleTab(WideTable wideTable) {
        //解析IP
        String uip = wideTable.getDcssip();
        if (StringUtils.isNotBlank(uip)) {
            IPSeekerExt.RegionInfo info = ipSeekerExt.analyticIp(uip);
            if (null != info) {
                wideTable.setDcssip(info.getCountry() + "," + info.getProvince() + "," + info.getCity());
            }
        }
        //解析domain
        String domain = wideTable.getUser_domain();
        if (StringUtils.isNotBlank(domain)) {
            wideTable.setUser_domain(LogConstants.UserDomain.getAlias(domain));
        }
    }

//    private static void handleUserAgent(AnalysedLog analysedLog) {
//        String csUserAgent = analysedLog.getCsUserAgent();
//        if (StringUtils.isNotBlank(csUserAgent)) {
//            UserAgentUtil.UserAgentInfo info = UserAgentUtil.analyticUserAgent(csUserAgent);
//            if (info != null) {
//                analysedLog.setCsUserAgent(info.toString());
//            }
//        }
//    }
}
