package com.zk.example.client;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 创建zookeeper连接，这个链接的过程是易步德，当连接成功后会出发watcher。
 * 我们使用CountDownLatch来控制等待连接建立完毕并打印zookeeper连接状态。
 * Created by hadoop on 2017/6/30.
 */
public class ZkSample implements Watcher {
    private static Logger LOGGER = Logger.getLogger(ZkSample.class);

    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws IOException {
        ZkSample we = new ZkSample();

        ZooKeeper zk = new ZooKeeper(ZkSample.zkPath, 10000, we);

        LOGGER.info("Zk state:" + zk.getState());

        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        LOGGER.info("Zk state:" + zk.getState());
    }

    @Override
    public void process(WatchedEvent event) {
        LOGGER.info("watcher=" + this.getClass().getName());
        LOGGER.info("path=" + event.getPath());
        LOGGER.info("event=" + event.getType().name());
        if (Event.KeeperState.SyncConnected == event.getState()) {
            connectedSemaphore.countDown();
        }
    }
}
