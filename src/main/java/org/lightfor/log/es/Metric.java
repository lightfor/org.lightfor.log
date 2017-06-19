package org.lightfor.log.es;

import java.util.Date;
import java.util.Map;

/**
 * 指标对象
 * Created by Light on 2017/6/15.
 */
public class Metric {

    private String id;
    private String ip;
    private String metric;
    private Map<String,String> tags;
    private String tagsStr;
    private float value;
    private Date time;
    private boolean needJudge;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public String getTagsStr() {
        return tagsStr;
    }

    public void setTagsStr(String tagsStr) {
        this.tagsStr = tagsStr;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public boolean isNeedJudge() {
        return needJudge;
    }

    public void setNeedJudge(boolean needJudge) {
        this.needJudge = needJudge;
    }
}
