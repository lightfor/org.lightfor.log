package org.lightfor.log.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * ElasticSearch Client
 * Created by Light on 2017/6/13.
 */
public class ElasticSearchClient {

    public static void main(String[] args) throws UnknownHostException {
        // on startup
        Settings settings = Settings.builder()
                //.put("client.transport.sniff", true) // dynamically add new hosts and remove old ones
                .put("cluster.name", "myClusterName").build();
        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("host1"), 9300));


        // on shutdown
        client.close();
    }
}
