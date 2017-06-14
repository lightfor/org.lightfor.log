package org.lightfor.log.es;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Elastic Search Test
 * Created by Light on 2017/6/14.
 */
public class ElasticSearchTest {

    private TransportClient init() throws UnknownHostException {
        Settings settings = Settings.builder()
                //.put("client.transport.sniff", true) // dynamically add new hosts and remove old ones
                .put("cluster.name", "elasticsearchProd").build();
        return new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("223.203.208.99"), 9300));

    }

    @Test
    public void index() throws Exception {
        TransportClient client = init();


        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();
        IndexResponse response = client.prepareIndex("twitter", "tweet", "1")
                .setSource(builder)
                .get();

        System.out.println(builder.string());
        System.out.println(response.status());

        client.close();
    }

    public void search() throws Exception{
        TransportClient client = init();

        SearchResponse response = client.prepareSearch("index1", "index2")
                .setTypes("type1", "type2")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.termQuery("multi", "test"))                 // Query
                .setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
                .setFrom(0).setSize(60).setExplain(true)
                .get();
    }
}
