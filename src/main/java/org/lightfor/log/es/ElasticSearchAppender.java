package org.lightfor.log.es;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.net.Inet4Address;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * ElasticSearchAppender
 * Created by Light on 2017/4/6.
 */
public class ElasticSearchAppender extends AppenderSkeleton {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ElasticSearchAppender.class);

    @Override
    protected void append(LoggingEvent loggingEvent) {
        if(running){
            que.offer(new InternalLogEvent(loggingEvent));
        }
    }

    @Override
    public void close() {
        running = false;
    }

    @Override
    public boolean requiresLayout() {
        return false;
    }

    static class InternalLogEvent {
        String threadName = "";
        String clazz = "";
        String method = "";
        String line = "";
        LoggingEvent eve;

        InternalLogEvent(LoggingEvent loggingEvent) {
            eve = loggingEvent;
            threadName = eve.getThreadName();
            LocationInfo locationInfo = eve.getLocationInformation();
            clazz = locationInfo.getClassName();
            method = locationInfo.getMethodName();
            line = locationInfo.getLineNumber();
        }
    }

    private ArrayBlockingQueue<InternalLogEvent> que = null;

    private volatile boolean running = false;

    private volatile Config config = null;

    private ScheduledExecutorService scheduledExecutorService = null;

    static private class Config {
        String name = "elastic";
        List<URI> uri = null;
        String cluster = "elasticsearch";
        String index = "logstash";
        String type = "log";
        RotateIndexType rotateIndexParam = RotateIndexType.DAY;
        String node = "local";
        int buffer = 5000;
        long expiry = -1;
    }

    @Override
    public void activateOptions() {
        if (!initConfig()) return;

        Thread thr = new Thread() {
            public void run() {
                worker();
            }
        };
        thr.setDaemon(true);
        thr.setName("ElasticAppender " + config.name);
        thr.start();
    }

    private boolean initConfig() {
        ResourceBundle rb;
        InputStream is = null;
        try {
            is = ElasticSearchAppender.class.getClassLoader().getResourceAsStream("elasticSearchAppender.properties");
            rb = new PropertyResourceBundle(is);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            if(is != null){
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


        String esSwitch = getPropertiesString("esSwitch", rb);
        running = "on".equalsIgnoreCase(esSwitch);
        if(!running){
            return running;
        }

        config = new Config();
        if (name == null) {
            return false;
        }
        config.name = name;
        String myHost = null;
        try {
            myHost = Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        config.node = getProp(getPropertiesString("node", rb), "node", myHost);
        config.cluster = getProp(getPropertiesString("elasticCluster", rb), "elasticCluster", "elasticsearch");
        String elasticLocal = getProp(getPropertiesString("elasticLocal", rb), "elasticLocal", "native://localhost:9300");
        String[] uriDat = elasticLocal.split(",");
        config.uri = new LinkedList<>();
        for (String act : uriDat) {
            config.uri.add(URI.create(act));
        }
        config.rotateIndexParam = RotateIndexType.NO;
        String indexRotate = getPropertiesString("indexRotate", rb);
        if (indexRotate == null) {
            config.rotateIndexParam = RotateIndexType.DAY;
        } else if (indexRotate.equalsIgnoreCase("NO")) {
            config.rotateIndexParam = RotateIndexType.NO;
        } else if (indexRotate.equalsIgnoreCase("DAY")) {
            config.rotateIndexParam = RotateIndexType.DAY;
        } else if (indexRotate.equalsIgnoreCase("HOUR")) {
            config.rotateIndexParam = RotateIndexType.HOUR;
        } else {
            config.rotateIndexParam = RotateIndexType.DAY;
        }

        String index = getPropertiesString("index", rb);
        if (index != null) {
            config.index = index;
        }
        String type = getPropertiesString("type", rb);
        if (type != null) {
            config.type = type;
        }
        String bufferSize = getPropertiesString("bufferSize", rb);
        if (bufferSize != null) {
            config.buffer = Integer.parseInt(bufferSize);
        }
        long factor = 24 * 60 * 60 * 1000L;
        String expiryUnit = getPropertiesString("expiryUnit", rb);
        if (expiryUnit != null) {
            if (expiryUnit.equalsIgnoreCase("w")) {
                factor = 7 * 24 * 60 * 60 * 1000L;
            } else if (expiryUnit.equalsIgnoreCase("d")) {
                factor = 24 * 60 * 60 * 1000L;
            } else if (expiryUnit.equalsIgnoreCase("h")) {
                factor = 60 * 60 * 1000L;
            } else if (expiryUnit.equalsIgnoreCase("m")) {
                factor = 60 * 1000L;
            } else if (expiryUnit.equalsIgnoreCase("s")) {
                factor = 1000L;
            } else if (expiryUnit.equalsIgnoreCase("msec")) {
                factor = 1000L;
            }
        }
        String expiry = getPropertiesString("expiry", rb);
        if (expiry != null) {
            Long expiryLong = Long.parseLong(expiry);
            if (expiryLong > 0) {
                config.expiry = expiryLong * factor;
            }
        }
        que = new ArrayBlockingQueue<>(config.buffer);

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                int size = que.size();

                if (size >= 1500 && size % 3 == 0) {
                    logger.error("队列中数量size={}", size);
                } else if (size > 4000){
                    logger.error("队列中数量过大size={}***************", size);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);

        return running;
    }

    private void worker() {
        Settings settings = settingsBuilder()
                .put("cluster.name", config.cluster)
                .put("network.server", false)
                .put("node.client", true)
                .put("client.transport.sniff", false)
                .put("client.transport.ping_timeout", "5s")
                .put("client.transport.ignore_cluster_name", false)
                .put("client.transport.nodes_sampler_interval", "5s")
                .build();

        logger.info("Start now ElasticAppender Thread");

        String resolvedIndex = null;
        long indexHour = 0;
        SimpleDateFormat formatDay = new SimpleDateFormat("yyyy.MM.dd");
        SimpleDateFormat formatElastic = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        Map<String, Object> eve = new HashMap<>();

        while (running) {
            TransportClient client = new TransportClient(settings, false);
            for (URI act : config.uri) {
                client.addTransportAddress(new InetSocketTransportAddress(act.getHost(), act.getPort()));
            }
            if (client.connectedNodes().isEmpty()) {
                client.close();
                logger.error("unable to connect to Elasticsearch cluster");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    logger.error("Elasticsearch Exception", e);
                }
                continue;
            }

            while (running) {
                InternalLogEvent ilog = null;
                try {
                    ilog = que.poll(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    logger.error("Elasticsearch Exception", e);
                }
                if (ilog == null) {
                    continue;
                }
                List<InternalLogEvent> logs = new LinkedList<>();
                logs.add(ilog);
                que.drainTo(logs, 50);
                BulkRequestBuilder bulkRequest = client.prepareBulk();
                for (InternalLogEvent ievent : logs) {
                    LoggingEvent event = ievent.eve;
                    if ((event.getTimeStamp() / 3600000L) != indexHour) {
                        indexHour = event.getTimeStamp() / 3600000L;
                        long localTime = event.getTimeStamp();
                        Date dd = new Date(localTime);
                        resolvedIndex = config.index + "-" + formatDay.format(dd);
                    }

                    eve.clear();
                    eve.put("@timestamp", formatElastic.format(new Date(event.getTimeStamp())));
                    eve.put("host", config.node);
                    eve.put("level", event.getLevel().toString());
                    eve.put("thread", ievent.threadName);
                    eve.put("class", ievent.clazz);
                    eve.put("method", ievent.method);
                    eve.put("line", ievent.line);
                    eve.put("message", event.getMessage());
                    for (Object key : event.getProperties().keySet()) {// key cannot contain '.'
                        eve.put(String.format("mdc_%s", key.toString()), event.getProperties().get(key).toString());
                    }
                    ThrowableInformation ti = event.getThrowableInformation();
                    if (ti != null) {
                        Throwable t = ti.getThrowable();
                        eve.put("className", t.getClass().getCanonicalName());
                        eve.put("stackTrace", getStackTrace(t));
                    }

                    IndexRequestBuilder d = client.prepareIndex(resolvedIndex, config.type).setSource(eve);
                    if (config.expiry > 0) {
                        d.setTTL(config.expiry);
                    }
                    bulkRequest.add(d);
                }
                try {
                    bulkRequest.execute();
                }catch (Exception e){
                    try{
                        client.close();
                    } catch (Exception ex){
                        logger.error("Elasticsearch Exception", ex);
                    }
                    logger.error("Elasticsearch Exception", e);
                    break;
                }
            }
        }
    }

    private String getStackTrace(Throwable aThrowable) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        aThrowable.printStackTrace(printWriter);
        return result.toString();
    }

    private static String getProp(String param, String name, String def) {
        if (param != null) {
            return param;
        }
        String str = System.getProperty(name);
        if (str != null) {
            return str;
        }
        str = System.getenv(name);
        if (str != null) {
            return str;
        }
        return def;
    }

    private String getPropertiesString(String key, ResourceBundle rb){
        if(rb.containsKey(key)){
            return rb.getString(key);
        }
        return null;
    }

}
