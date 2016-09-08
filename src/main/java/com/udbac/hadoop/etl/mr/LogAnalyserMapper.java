package com.udbac.hadoop.etl.mr;

import com.udbac.hadoop.common.DefinedKey;
import com.udbac.hadoop.util.SplitValueBuilder;
import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 分析初始SDC日志的Mapper
 */
public class LogAnalyserMapper extends Mapper<LongWritable, Text, DefinedKey, Text> {
    private final Logger logger = Logger.getLogger(LogAnalyserMapper.class);
    private DefinedKey definedKey = new DefinedKey();
    private String inputPath = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        inputPath = configuration.get("inputPath");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            if (null == value) {
                return;
            }
            String[] tokens = value.toString().split("\\|");
            String date_time = handleTime(tokens[1]);
            String date = date_time.split(" ")[0];
            String time = date_time.split(" ")[1].replace(":", "");
            SplitValueBuilder svb = new SplitValueBuilder();
            svb.add(tokens[0]).add(date).add(time);

            for (int i = 2; i < tokens.length; i++) {
                svb.add(tokens[i]);
            }
            definedKey.setDeviceId(tokens[0]);
            definedKey.setTimeStr(TimeUtil.timeToInt(time));
            if (inputPath.contains(date)) {
                context.write(definedKey, new Text(svb.build()));
            }
        } catch (Exception e) {
            this.logger.error("处理SDCLOG出现异常，数据:" + value);
        }
    }

    private static String handleTime(String dateTime) {
        String realtime = null;
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date date = dateFormat.parse(dateTime);
            calendar.setTime(date);
            calendar.add(Calendar.HOUR_OF_DAY, 7);
            calendar.add(Calendar.MINUTE, 59);
            calendar.add(Calendar.SECOND, 59);
            realtime = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(calendar.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return realtime;
    }

}
