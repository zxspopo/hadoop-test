package com.yonyou.cloud.mail.utils;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.shield.ShieldPlugin;

import javax.mail.MessagingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ESRepository {

    private String cluster;

    private String username;

    private String password;

    private String hosts;

    private volatile boolean isClusterOn;

    private volatile TransportClient esclient;

    private volatile BulkProcessor bulkProcessor;

    public ESRepository() {}

    public ESRepository(String cluster, String username, String password, String hosts) {
        this.cluster = cluster;
        this.username = username;
        this.password = password;
        this.hosts = hosts;
    }

    public TransportClient getClient() {
        if (esclient == null) {
            synchronized (ESRepository.class) {
                if (esclient == null) {
                    initESClient();
                }
            }
        }
        if (!isClusterOn) {
            throw new RuntimeException("the cluster no node avaliable");
        }
        return esclient;
    }

    private void initESClient() {
        Settings.Builder builder = Settings.settingsBuilder().put("client.transport.sniff", false);
        if (StringUtils.isNotBlank(cluster)) {
            builder.put("cluster.name", cluster);
        }
        Settings settings = builder.put("shield.user", username + ":" + password).build();
        esclient = TransportClient.builder().addPlugin(ShieldPlugin.class).settings(settings).build();
        System.out.println("hosts is:" + hosts);
        String[] hostsArray = hosts.split(",");
        for (String host : hostsArray) {
            String[] hp = host.split(":");
            String h = null, p = null;
            if (hp.length == 2) {
                h = hp[0];
                p = hp[1];
            } else if (hp.length == 1) {
                h = hp[0];
                p = "9300";
            }
            try {
                esclient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(h), Integer
                        .parseInt(p)));
            } catch (UnknownHostException e) {
                throw new RuntimeException(e.getMessage(), e.getCause());
            }
        }
        isClusterOn = true;
        ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.submit(new ClusterMonitor(esclient));

    }

    class ClusterMonitor implements Runnable {

        private TransportClient transportClient;

        public ClusterMonitor(TransportClient client) {
            this.transportClient = client;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    ActionFuture<ClusterHealthResponse> healthFuture =
                            transportClient.admin().cluster().health(Requests.clusterHealthRequest());
                    ClusterHealthResponse healthResponse = healthFuture.get(5, TimeUnit.SECONDS);

                    if (healthResponse.getStatus() == ClusterHealthStatus.RED) {// es集群处于不健康状态给提示
                        MailUtils.sendMail("*******", hosts + " ES 集群状态为RED!!");
                    }

                    isClusterOn = true;
                } catch (Throwable t) {
                    if (t instanceof NoNodeAvailableException) {// 集群不可用
                        try {
                            MailUtils.sendMail("*******", hosts + " ES 集群状态不可用!!");
                        } catch (MessagingException e) {
                            e.printStackTrace();
                        }
                        isClusterOn = false;
                    } else {
                        isClusterOn = true;
                    }
                }
                try {
                    Thread.sleep(3000);// FIXME
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }
    }

    public boolean isClusterOn() {
        return isClusterOn;
    }

}
