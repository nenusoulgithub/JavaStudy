package com.zk.example.curator;

import com.alibaba.fastjson.JSON;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Created by hadoop on 2017/7/13.
 */
public class CuratorClientSample {
    private static Logger LOGGER = Logger.getLogger(CuratorClientSample.class);

    private static final String zkPath = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183";

    private CuratorFramework client = null;

    private CuratorClientSample() {
        RetryPolicy policy1 = new RetryNTimes(1000, 3);
        RetryPolicy policy2 = new RetryOneTime(1000);
        RetryPolicy policy3 = new RetryUntilElapsed(10000, 2000);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        client = CuratorFrameworkFactory
                .builder()
                .connectString(CuratorClientSample.zkPath)
                .connectionTimeoutMs(15 * 1000)
                .sessionTimeoutMs(60 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("zk-curator")
                .build();

        client.start();
    }

    private void close() {
        if (client != null)
            this.client.close();
    }

    private void createNode(String path, byte[] data) throws Exception {
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                .forPath(path, data);
        LOGGER.info("创建节点成功：" + path);
    }

    private void deleteNode(String path, int version) throws Exception {
        client.delete()
                //用重试的方式来保证删除成功:FailedDeleteManager
                .guaranteed()
                //如果有孩子节点删除孩子节点
                .deletingChildrenIfNeeded()
                //指定待删除节点的版本
                .withVersion(version)
                //删除节点路径
                .forPath(path);
        LOGGER.info("删除节点成功：" + path);
    }

    private Stat readNode(String path) throws Exception {
        Stat stat = new Stat();
        byte[] data = client.getData().storingStatIn(stat).forPath(path);
        LOGGER.info("节点数据：" + new String(data));
        LOGGER.info("节点属性：" + JSON.toJSONString(stat));
        return stat;
    }

    private void updateNode(String path, byte[] data, int version) throws Exception {
        client.setData().withVersion(version).forPath(path, data);
        LOGGER.info("更新节点成功：" + path);
    }

    private boolean checkExists(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return stat != null;
    }

    private void deleteNodeInBackground(String path, int version) throws Exception {
        client.delete().guaranteed()
                .deletingChildrenIfNeeded()
                .withVersion(version)
                .inBackground(new DeleteCallback())
                .forPath(path);
    }

    private void getChildren(String path) throws Exception {
        List<String> children =  client.getChildren().usingWatcher(new MyCuratorWatcher()).forPath(path);
        for (String child : children) {
            LOGGER.info("child=" + child);
        }
    }

    /**
     * NodeCache
     • 监听数据节点的内容变更
     • 监听节点的创建，即如果指定的节点不存在，则节点创建后，会触发这个监听
     * @param path 路径
     */
    private void addNodeDataWatcher(String path) throws Exception {
        final NodeCache nodeC = new NodeCache(client, path);

        nodeC.start();
        nodeC.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                String data = new String(nodeC.getCurrentData().getData());
                LOGGER.info("path=" + nodeC.getCurrentData().getPath() + ":data=" + data);
            }
        });
    }

    /**
     *
     * @param path
     * @throws Exception
     */
    private void addChildrenWatcher(String path) throws Exception {
        final PathChildrenCache cache = new PathChildrenCache(client, path, true);
        /**
         * StartMode.BUILD_INITIAL_CACHE    //同步初始化客户端的cache，及创建cache后，就从服务器端拉入对应的数据
         * StartMode.NORMAL                 //异步初始化cache
         * StartMode.POST_INITIALIZED_EVENT //异步初始化，初始化完成触发事件PathChildrenCacheEvent.Type.INITIALIZED
         */
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);

        LOGGER.info(cache.getCurrentData().size());

        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                if(event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)){
                    System.out.println("客户端子节点cache初始化数据完成");
                    System.out.println("size="+cache.getCurrentData().size());
                }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
                    System.out.println("添加子节点:"+event.getData().getPath());
                    System.out.println("修改子节点数据:"+new String(event.getData().getData()));
                }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)){
                    System.out.println("删除子节点:"+event.getData().getPath());
                }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)){
                    System.out.println("修改子节点数据:"+event.getData().getPath());
                    System.out.println("修改子节点数据:"+new String(event.getData().getData()));
                }
            }
        });
    }


    public static void main(String[] args) {
        CuratorClientSample cs = new CuratorClientSample();

        String path = "/test-creating/node";
        String data = "node";

        try {
            if (cs.checkExists(path)) {
                LOGGER.info("节点存在，先删除节点。");
                Stat stat = cs.readNode(path);
                cs.deleteNode(path, stat.getVersion());
                cs.createNode(path, data.getBytes());
            } else {
                LOGGER.info("节点不存在，直接创建节点。");
                cs.createNode(path, data.getBytes());
            }
            Stat stat = cs.readNode(path);
            LOGGER.info("成功创建节点：" + JSON.toJSONString(stat));

            LOGGER.info("更新节点数据" );
            cs.updateNode(path, "nodenode".getBytes() , stat.getVersion());
            stat = cs.readNode(path);

            //cs.createNode("/test-watcher/node1", data.getBytes());
            //cs.createNode("/test-watcher/node2", data.getBytes());
            cs.getChildren("/test-watcher");
            cs.addNodeDataWatcher("/test-watcher");
            cs.addChildrenWatcher("/test-watcher");

            LOGGER.info("后台删除节点");
            cs.deleteNodeInBackground(path, stat.getVersion());

            Thread.sleep(120000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cs.close();
        }
    }

    private static class MyCuratorWatcher implements CuratorWatcher {
        private static Logger LOGGER = Logger.getLogger(MyCuratorWatcher.class);

        @Override
        public void process(WatchedEvent event) throws Exception {
            LOGGER.info("WatchedEvent：path=" + event.getPath());
            LOGGER.info("WatchedEvent：event type=" + event.getType());
        }
    }

    private static class DeleteCallback implements BackgroundCallback {
        private static Logger LOGGER = Logger.getLogger(DeleteCallback.class);

        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
            LOGGER.info("异步删除节点回调：path=" + event.getPath());
            LOGGER.info("异步删除节点回调：data=" + event.getData());
            LOGGER.info("异步删除节点回调：event type=" + event.getType());
            LOGGER.info("异步删除节点回调：event code=" + event.getResultCode());
        }
    }
}
