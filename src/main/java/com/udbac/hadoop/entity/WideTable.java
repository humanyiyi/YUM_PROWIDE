package com.udbac.hadoop.entity;

import com.udbac.hadoop.util.SplitValueBuilder;

import java.lang.reflect.Field;
import java.math.BigDecimal;

/**
 * Created by chaoslane@126.com on 2016/7/25.
 */
public class WideTable {

    private String deviceId;
    private String date;
    private String time;
    private String dcssip;
    private String brand_new_user;
    private String user_domain;
    private String spreadid;
    private String wt_login;
    private String wt_menu;
    private String wt_user;
    private String wt_cart;
    private String wt_suc;
    private String wt_pay;
    private String wt_event;
    private BigDecimal duration;

    /**
     * 把map输出到reduce的一行字符串 转化为 WideTable对象
     *
     * @param mapinput
     * @return WideTable
     */
    public static WideTable parse(String mapinput) {
        WideTable wideTable = null;
        String[] tokens = mapinput.split("\\|");
        if (tokens.length == 14) {
            wideTable = new WideTable();
            Field[] field = wideTable.getClass().getDeclaredFields();
            for (int i = 0; i < 14; i++) {
                try {
                    field[i].set(wideTable, tokens[i]);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
            return wideTable;
        }
        return null;
    }


    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDcssip() {
        return dcssip;
    }

    public void setDcssip(String dcssip) {
        this.dcssip = dcssip;
    }

    public String getBrand_new_user() {
        return brand_new_user;
    }

    public void setBrand_new_user(String brand_new_user) {
        this.brand_new_user = brand_new_user;
    }

    public String getUser_domain() {
        return user_domain;
    }

    public void setUser_domain(String user_domain) {
        this.user_domain = user_domain;
    }

    public String getSpreadid() {
        return spreadid;
    }

    public void setSpreadid(String spreadid) {
        this.spreadid = spreadid;
    }

    public String getWt_login() {
        return wt_login;
    }

    public void setWt_login(String wt_login) {
        this.wt_login = wt_login;
    }

    public String getWt_menu() {
        return wt_menu;
    }

    public void setWt_menu(String wt_menu) {
        this.wt_menu = wt_menu;
    }

    public String getWt_user() {
        return wt_user;
    }

    public void setWt_user(String wt_user) {
        this.wt_user = wt_user;
    }

    public String getWt_cart() {
        return wt_cart;
    }

    public void setWt_cart(String wt_cart) {
        this.wt_cart = wt_cart;
    }

    public String getWt_suc() {
        return wt_suc;
    }

    public void setWt_suc(String wt_suc) {
        this.wt_suc = wt_suc;
    }

    public String getWt_pay() {
        return wt_pay;
    }

    public void setWt_pay(String wt_pay) {
        this.wt_pay = wt_pay;
    }

    public String getWt_event() {
        return wt_event;
    }

    public void setWt_event(String wt_event) {
        this.wt_event = wt_event;
    }

    public BigDecimal getDuration() {
        return duration;
    }

    public void setDuration(BigDecimal duration) {
        this.duration = duration;
    }

    @Override
    public String toString() {
        SplitValueBuilder svb = new SplitValueBuilder();
        svb.add(deviceId).add(date).add(time).add(dcssip).add(brand_new_user).add(user_domain).add(spreadid)
                .add(wt_login).add(wt_menu).add(wt_user).add(wt_cart).add(wt_suc).add(wt_pay).add(wt_event).add(duration);
        return svb.build();
    }
}
