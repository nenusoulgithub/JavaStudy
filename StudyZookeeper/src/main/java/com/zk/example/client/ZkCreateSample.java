package com.zk.example.client;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 分别zk节点
 *
 * Created by hadoop on 2017/7/3.
 */
public class ZkCreateSample {
    private static Logger LOGGER = Logger.getLogger(ZkCreateSample.class);

    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    private ZooKeeper zk = null;

    public ZkCreateSample() throws IOException {
        zk = new ZooKeeper(ZkCreateSample.zkPath, 10000, new Watcher() {
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

    private String createNode(String path, byte[] data, List<ACL> acls) {
        String result = null;
        try {
            result = zk.create(path, data, acls, CreateMode.PERSISTENT);
            LOGGER.info("创建节点" + result + "成功！");
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return result;
    }

    public List<ACL> getIpAcl() {
        List<ACL> acls = new ArrayList<ACL>();
        Id ipId = new Id("ip", "127.0.0.1");
        acls.add(new ACL(ZooDefs.Perms.ALL, ipId));
        return acls;
    }

    public List<ACL> getDigestAcl() {
        List<ACL> acls = new ArrayList<ACL>();
        Id digestId = new Id("digest", "zk-client-node4:ibXJkhWzzKfbdQeH6g8s0ky+fLA=");
        acls.add(new ACL(ZooDefs.Perms.ALL, digestId));
        return acls;
    }

    public static void main(String[] args) throws IOException {
        ZkCreateSample sample = new ZkCreateSample();

        //创建Scheme:world的节
        //sample.createNode("/zk-client/node1", "node1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        //创建Scheme:auth的节点
        //sample.getZk().addAuthInfo("digest", "zk-client-node1:111111".getBytes());
        //sample.createNode("/zk-client/node2", "node2".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL);

        //创建Scheme:ip的节点
        //sample.createNode("/zk-client/node3", "node3".getBytes(), sample.getIpAcl());

        //创建Scheme:digest的节点
        //sample.createNode("/zk-client/node4", "node4".getBytes(), sample.getDigestAcl());
    }

    public ZooKeeper getZk() {
        return zk;
    }
}
