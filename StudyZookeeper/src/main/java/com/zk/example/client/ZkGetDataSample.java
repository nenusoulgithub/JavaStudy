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
public class ZkGetDataSample {
    private static Logger LOGGER = Logger.getLogger(ZkGetDataSample.class);

    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zk = null;

    public ZkGetDataSample() throws IOException {
        zk = new ZooKeeper(ZkGetDataSample.zkPath, 10000, new Watcher() {
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

    private byte[] getData(String path, Stat stat) throws KeeperException, InterruptedException {
        byte[] data = zk.getData(path, false, stat);
        LOGGER.info("stat:" + JSON.toJSONString(stat));
        LOGGER.info("data:" + JSON.toJSONString(new String(data)));
        return data;
    }

    private void getDataAsync(String path) {
        zk.addAuthInfo("digest", "zk-client-node4:111111".getBytes());
        zk.getData(path, false, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                LOGGER.info("path:" + JSON.toJSONString(path));
                LOGGER.info("ctx:" + JSON.toJSONString(ctx));
                LOGGER.info("stat:" + JSON.toJSONString(stat));
                LOGGER.info("data:" + JSON.toJSONString(new String(data)));
                connectedSemaphore.countDown();
            }
        }, "上下文信息");
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZkGetDataSample sample = new ZkGetDataSample();

        LOGGER.info("-----------------------------------------------");
        Stat stat = new Stat();
        byte[] data = sample.getData("/zk-client/node1", stat);
        LOGGER.info("stat:" + JSON.toJSONString(stat));
        LOGGER.info("data:" + JSON.toJSONString(new String(data)));
        LOGGER.info("-----------------------------------------------");

        LOGGER.info("-----------------------------------------------");
        connectedSemaphore = new CountDownLatch(1);
        sample.getDataAsync("/zk-client/node4");
        connectedSemaphore.await();
        LOGGER.info("-----------------------------------------------");
    }
}
