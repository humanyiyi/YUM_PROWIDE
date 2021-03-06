package com.udbac.hadoop.common;

import com.udbac.hadoop.util.TimeUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 * 文件夹过滤 可以加入到job中
 * 判断文件夹名是否为昨日日期 暂时未启用
 */

public class TextPathFilter extends Configured implements PathFilter {
    @Override
    public boolean accept(Path path) {
        String date1 = getConf().get(LogConstants.RUNNING_DATE_PARAMES);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(getConf());
            FileStatus fileStatus = fs.getFileStatus(path);
            if (fileStatus.isDirectory()) {
                String name = path.getName();
                if (TimeUtil.isValidateRunningDate(name)) {//文件夹名字 yyyy-MM-dd
                    if (name.equals(date1)) {//目录名字是否为前一天日期 yyyy-MM-dd
                        return true;
                    }
                }
            }
            if (fileStatus.isFile()) {
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }
}
