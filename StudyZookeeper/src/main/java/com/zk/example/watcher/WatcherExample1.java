package com.zk.example.watcher;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * 在每次Watch触发之后再重新注册Watcher才能让Watcher持续生效
 *
 * Created by hadoop on 2017/6/12.
 */
public class WatcherExample1 implements Watcher {
    private static Logger LOGGER = Logger.getLogger(WatcherExample1.class);
    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    public static void main(String[] args) throws InterruptedException {
        WatcherExample1 we = new WatcherExample1();

        try {
            ZooKeeper zk = new ZooKeeper(WatcherExample1.zkPath, 10000, we);

            MyWathcer wathcer = new MyWathcer(zk);

            zk.getData("/zk-watchtest/node1", wathcer, null);

            Thread.sleep(60000);
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

class MyWathcer implements Watcher {
    private static Logger LOGGER = Logger.getLogger(MyWathcer.class);

    private ZooKeeper zk;

    MyWathcer(ZooKeeper zk) {
        this.zk = zk;
    }

    @Override
    public void process(WatchedEvent event) {
        LOGGER.info("watcher=" + this.getClass().getName());
        LOGGER.info("path=" + event.getPath());
        LOGGER.info("event=" + event.getType().name());

        MyWathcer wathcer = new MyWathcer(zk);

        try {
            zk.getData(event.getPath(), wathcer, null);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}