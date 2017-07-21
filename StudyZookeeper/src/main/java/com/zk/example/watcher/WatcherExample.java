package com.zk.example.watcher;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Watcher注册一次只能作用一次
 *
 * Created by hadoop on 2017/6/12.
 */
public class WatcherExample implements Watcher {
    private static Logger LOGGER = Logger.getLogger(WatcherExample.class);
    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static void main(String[] args) throws InterruptedException {
        WatcherExample we = new WatcherExample();

        try {
            ZooKeeper zk = new ZooKeeper(WatcherExample.zkPath, 10000, we);
            zk.getChildren("/zk-watchtest", true);
            Thread.sleep(30000);
        } catch (IOException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        LOGGER.info("watcher=" + this.getClass().getName());
        LOGGER.info("path=" + event.getPath());
        LOGGER.info("event=" + event.getType().name());
    }
}


