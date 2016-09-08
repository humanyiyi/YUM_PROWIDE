package com.udbac.hadoop.common;

import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by root on 2016/7/25.
 */
public class DefinedKey implements WritableComparable<DefinedKey> {
    private static final Logger logger = Logger.getLogger(DefinedKey.class);
    private String deviceId;
    private int timeStr;

    public DefinedKey() {
        super();
    }

    public DefinedKey(String deviceId, int timeStr) {
        super();
        this.deviceId = deviceId;
        this.timeStr = timeStr;
    }

    @Override
    public int compareTo(DefinedKey o) {
        return this.deviceId.compareTo(o.getDeviceId());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        deviceId = in.readUTF();
        timeStr = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!out.equals(null)) {
            out.writeUTF(deviceId);
            out.writeInt(timeStr);
        }
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public int getTimeStr() {
        return timeStr;
    }

    public void setTimeStr(int timeStr) {
        this.timeStr = timeStr;
    }
}
