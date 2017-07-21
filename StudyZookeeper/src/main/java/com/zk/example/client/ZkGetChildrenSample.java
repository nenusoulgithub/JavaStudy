package com.zk.example.client;

import com.alibaba.fastjson.JSON;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by hadoop on 2017/7/7.
 */
public class ZkGetChildrenSample {
    private static Logger LOGGER = Logger.getLogger(ZkGetChildrenSample.class);

    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zk = null;

    private ZkGetChildrenSample() throws IOException {
        zk = new ZooKeeper(ZkGetChildrenSample.zkPath, 10000, new Watcher() {
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

    private List<String> getChildren(String path) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, false);
        LOGGER.info("Children:" + JSON.toJSONString(children));
        return children;
    }

    private List<String> getChildren(String path, Stat stat) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, false, stat);
        LOGGER.info("Stat:" + JSON.toJSONString(stat));
        LOGGER.info("Children:" + JSON.toJSONString(children));
        return children;
    }

    private void getChildrenAsync1(String path) throws KeeperException, InterruptedException {
        zk.getChildren(path, false, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                LOGGER.info("ctx:" + ctx);
                LOGGER.info("Children:" + JSON.toJSONString(children));
                connectedSemaphore.countDown();
            }
        }, "上下文信息");
    }

    private void getChildrenAsync2(String path) throws KeeperException, InterruptedException {
        zk.getChildren(path, false, new AsyncCallback.Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                LOGGER.info("ctx:" + ctx);
                LOGGER.info("Stat:" + JSON.toJSONString(stat));
                LOGGER.info("Children:" + JSON.toJSONString(children));
                connectedSemaphore.countDown();
            }
        }, "上下文信息");
    }


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZkGetChildrenSample sample = new ZkGetChildrenSample();

        //两个同步方法
        LOGGER.info("-----------------------------------------------");
        List<String> children1 = sample.getChildren("/zk-client");
        LOGGER.info("Children:" + JSON.toJSONString(children1));
        LOGGER.info("-----------------------------------------------");

        LOGGER.info("-----------------------------------------------");
        Stat stat = new Stat();
        List<String> children3 = sample.getChildren("/zk-client", stat);
        LOGGER.info("Stat:" + JSON.toJSONString(stat));
        LOGGER.info("Children:" + JSON.toJSONString(children3));
        LOGGER.info("-----------------------------------------------");

        //两个异步方法
        LOGGER.info("-----------------------------------------------");
        connectedSemaphore = new CountDownLatch(1);
        sample.getChildrenAsync1("/zk-client");
        connectedSemaphore.await();
        LOGGER.info("-----------------------------------------------");

        LOGGER.info("-----------------------------------------------");
        connectedSemaphore = new CountDownLatch(1);
        sample.getChildrenAsync2("/zk-client");
        connectedSemaphore.await();
        LOGGER.info("-----------------------------------------------");

    }
}
