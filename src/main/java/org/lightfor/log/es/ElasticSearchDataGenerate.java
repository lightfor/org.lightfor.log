package org.lightfor.log.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * ElasticSearch Data Generate
 * Created by Light on 2017/6/19.
 */
public class ElasticSearchDataGenerate {

    public static void main(String[] args) throws Exception {

        RestClient restClient = RestClient.builder(new HttpHost("192.168.1.66", 9200, "http")).build();

        Map<String, String> tags = new HashMap<>();
        tags.put("type", "test");
        tags.put("service", "local");
        for(int i = 0 ; i < 100 ; i++){
            Metric metric = new Metric();
            metric.setId(UUID.randomUUID().toString().replaceAll("-", ""));
            metric.setIp("10.10.0." + String.valueOf(i % 2));
            metric.setMetric("test" + String.valueOf(i % 3));
            tags.put("mod", String.valueOf(i % 5));
            metric.setTags(tags);
            StringBuilder sb = new StringBuilder(300);
            for(String key : metric.getTags().keySet()){
                sb.append(key).append("=").append(metric.getTags().get(key)).append(",");
            }
            if (sb.length() > 0){
                sb.deleteCharAt(sb.length() - 1);
            }
            metric.setTagsStr(sb.toString());
            metric.setValue((float) (Math.random() * 100));
            metric.setTime(new Date(System.currentTimeMillis() - i * 5 * 60 * 1000));
            metric.setNeedJudge(true);
            HttpEntity entity = new NStringEntity(JSON.toJSONString(metric, SerializerFeature.UseISO8601DateFormat), ContentType.APPLICATION_JSON);
            Response indexResponse = restClient.performRequest( "PUT", "/metrictest/test/"+metric.getId(), Collections.<String, String>emptyMap(), entity);
        }
        restClient.close();
    }
}
