package com.zk.example.client;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *
 * Created by hadoop on 2017/7/7.
 */
public class ZkSetDataSample {
    private static Logger LOGGER = Logger.getLogger(ZkSetDataSample.class);

    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zk = null;

    private ZkSetDataSample() throws IOException {
        zk = new ZooKeeper(ZkSetDataSample.zkPath, 10000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                LOGGER.info("watcher=" + this.getClass().getName());
                LOGGER.info("path=" + event.getPath());
                LOGGER.info("event=" + event.getType().name());
                if (Watcher.Event.KeeperState.SyncConnected == event.getState()) {
                    connectedSemaphore.countDown();
                }
            }
        });

        LOGGER.info("Zk state:" + zk.getState());

        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOGGER.info("Zk state:" + zk.getState());
    }

    private Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return zk.setData(path, data, version);
    }

    private void setDataAsync(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        zk.setData(path, data, version, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                LOGGER.info("path:" + JSON.toJSONString(path));
                LOGGER.info("ctx:" + JSON.toJSONString(ctx));
                LOGGER.info("stat:" + JSON.toJSONString(stat));
                connectedSemaphore.countDown();
            }
        }, "上下文信息");
    }

    private byte[] getData(String path, Stat stat) throws KeeperException, InterruptedException {
        byte[] data = zk.getData(path, false, stat);
        LOGGER.info("stat:" + JSON.toJSONString(stat));
        LOGGER.info("data:" + JSON.toJSONString(new String(data)));
        return data;
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZkSetDataSample sample = new ZkSetDataSample();

        LOGGER.info("-----------------------------------------------");
        Stat stat = new Stat();
        byte[] data = sample.getData("/zk-client/node1", stat);
        LOGGER.info("stat:" + JSON.toJSONString(stat));
        LOGGER.info("data:" + JSON.toJSONString(new String(data)));
        LOGGER.info("-----------------------------------------------");

        LOGGER.info("-----------------------------------------------");
        stat = sample.setData("/zk-client/node1", "node1".getBytes(), stat.getVersion());
        LOGGER.info("stat:" + JSON.toJSONString(stat));
        LOGGER.info("-----------------------------------------------");

        LOGGER.info("-----------------------------------------------");
        connectedSemaphore = new CountDownLatch(1);
        sample.setDataAsync("/zk-client/node1", "node1".getBytes(), stat.getVersion());
        connectedSemaphore.await();
        LOGGER.info("-----------------------------------------------");
    }
}
