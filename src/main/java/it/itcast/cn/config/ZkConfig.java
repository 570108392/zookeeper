package it.itcast.cn.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @createUser: 张鹏
 * @createTime: 2020/10/29
 * @descripton:
 **/
@Configuration
@Slf4j
public class ZkConfig {

    @Value("${zookeeper.address}")
    private String zkAddress;
    @Value("${zookeeper.timeout}")
    private Integer timeout;

    @Bean(name = "zkClient")
    public ZooKeeper getClient(){

        ZooKeeper zkClient = null;

        try {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            zkClient = new ZooKeeper(zkAddress, timeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (Event.KeeperState.SyncConnected == event.getState()){
                        log.info("zk 连接成功！");
                        countDownLatch.countDown();
                    }
                    else if (Event.KeeperState.Disconnected == event.getState())
                        log.info("zk 连接失败");
                }
            });

            countDownLatch.await();
            log.info("zk 初始化客户端连接成功");

        } catch (Exception e) {
            log.info("zk 初始化客户端连接失败");
            e.printStackTrace();
        }
        return zkClient;
    }
}
