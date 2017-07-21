package com.zk.example.client;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by hadoop on 2017/7/6.
 */
public class ZkDeleteSample {
    private static Logger LOGGER = Logger.getLogger(ZkDeleteSample.class);

    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zk = null;

    public ZkDeleteSample() throws IOException {
        zk = new ZooKeeper(ZkDeleteSample.zkPath, 10000, new Watcher() {
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

    public void deleteNodeWithSync(String path, int version) {
        try {
            zk.delete(path, version);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public void deleteNodeWithAsync(String path, int version) {
        zk.delete(path, version, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                LOGGER.info("删除节点:" + path);
                LOGGER.info("上下文:" + ctx);
            }
        }, "我就是上下文信息");
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ZkDeleteSample sample = new ZkDeleteSample();

        sample.deleteNodeWithSync("/zk-client/node5", 0);

        Thread.sleep(3000);

        sample.deleteNodeWithAsync("/zk-client/node6", 0);

        Thread.sleep(3000);
    }
}
