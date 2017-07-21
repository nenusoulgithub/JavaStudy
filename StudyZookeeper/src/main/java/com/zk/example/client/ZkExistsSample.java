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
public class ZkExistsSample {
    private static Logger LOGGER = Logger.getLogger(ZkExistsSample.class);

    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zk = null;

    private ZkExistsSample() throws IOException {
        zk = new ZooKeeper(ZkExistsSample.zkPath, 10000, new Watcher() {
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

    private Stat exists(String path) throws KeeperException, InterruptedException {
        return zk.exists(path, false);
    }

    private void existsAsync(String path) {
        zk.exists(path, false, new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                LOGGER.info(stat == null ? "节点不存在":"节点存在");
                LOGGER.info("path:" + JSON.toJSONString(path));
                LOGGER.info("ctx:" + JSON.toJSONString(ctx));
                LOGGER.info("stat:" + JSON.toJSONString(stat));
                connectedSemaphore.countDown();
            }
        }, "上下文信息");
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZkExistsSample sample = new ZkExistsSample();

        LOGGER.info("-----------------------------------------------");
        Stat stat = sample.exists("/zk-client/node1");
        LOGGER.info(stat == null ? "节点不存在":"节点存在");
        LOGGER.info("stat:" + JSON.toJSONString(stat));
        LOGGER.info("-----------------------------------------------");

        LOGGER.info("-----------------------------------------------");
        connectedSemaphore = new CountDownLatch(1);
        sample.existsAsync("/zk-client/node9");
        connectedSemaphore.await();
        LOGGER.info("-----------------------------------------------");
    }
}
