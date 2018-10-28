/**
 * Copyright (C), 2015-2018, XXX有限公司
 * FileName: demo
 * Author:   zoujie
 * Date:     2018/10/21 上午10:46
 * Description:
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
package com.org.ch14;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 〈一句话功能简述〉<br> 
 * 〈〉
 *
 * @author zoujie
 * @create 2018/10/21
 * @since 1.0.0
 */
public class CreateGroup implements Watcher {

    private static final int SESSION_TIMEOUT = 5000;

    private ZooKeeper zk;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    /**
     * 实例化一格zookeeper对象
     * @param hosts zk地址
     */
    public void connect(String hosts) throws IOException, InterruptedException {
        //第一参数为地址和端口号，默认端口号为2181，第二个参数为会话超时参数（毫秒），
        //第三个参数为Watcher对象用于接收Zookeeper的回调，获取各种时间通知
        zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    /**
     * 当客户端与Zookeeper服务建立链接后，该方法被调用
     * 锁存起（latch）被创建时带有一个值为1的计数器，用户表示在它释放所有等待
     * 线程之前需要发生的时间书，在调用一次countDown()方法之后，计数器的值变为0
     * 则await()方法返回
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
    }

    /**
     * create方法主要是用来创建Zookeeper的znode
     * @param groupName 路径
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void create(String groupName) throws KeeperException,
            InterruptedException {
        String path = "/" + groupName;
        //路径，znode的内容，访问控制列表ACL，znode的类型：持久和短暂
        //znode的客户断开连接时，无论是明确断开还是其他任何原因终止，短暂znode都会被Zookeeper服务删除
        //持久的znode则与之相反
        String createdPath = zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        System.out.println("Created" + createdPath);
    }

    public void close() throws InterruptedException {
        zk.close();
    }

    public static void main(String[] args) throws Exception {
        String[] aa = {"localhost","zoo"};
        CreateGroup createGroup = new CreateGroup();
        createGroup.connect(aa[0]);
        createGroup.create(aa[1]);
        createGroup.close();
    }

}
