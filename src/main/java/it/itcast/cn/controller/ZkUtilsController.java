package it.itcast.cn.controller;

import it.itcast.cn.utils.FixedThreadPool;
import it.itcast.cn.utils.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @createUser: 张鹏
 * @createTime: 2020/10/29
 * @descripton:
 **/
@RestController
@RequestMapping("zk")
@Slf4j
public class ZkUtilsController {

    @Autowired
    private ZkUtil zkUtil;
    @GetMapping("exist")
    Stat exist(String path){
        return zkUtil.exists(path,false);
    }

    /**
     * 判断节点是否存在，并监听节点 创建 删除 修改
     * @return
     */
    @GetMapping("exists")
    Boolean exists(String path){
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (Event.EventType.NodeCreated == watchedEvent.getType())
                    log.info("节点{}创建成功",path);
                else if (Event.EventType.NodeDataChanged == watchedEvent.getType())
                    log.info("节点{}更新成功",path);
                else if (Event.EventType.NodeDeleted == watchedEvent.getType())
                    log.info("节点{}删除成功",path);

                //监听只是一次性 设置监听成功后继续监听机制
                exists(path);
            }
        };
        return zkUtil.exists(path,watcher);
    }

    @GetMapping("create")
    String create(String path){
        return zkUtil.createNode(path,path, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }
    @GetMapping("update")
    Boolean update(String path){
        return zkUtil.updateNode(path,path+"xiugai");
    }

    @GetMapping("delete")
    Boolean delete(String path){
        return zkUtil.deleteNode(path);
    }


    @GetMapping("uuid")
    String uuid(){
        return zkUtil.getId();
    }

    private void sell(Integer i){
        System.out.println("ID号："+i+"的人购票开始");
        // 线程随机休眠数毫秒，模拟现实中的费时操作

        int sleepMillis = new Random().nextInt(5) * 1000;
        try {
            //代表复杂逻辑执行了一段时间
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("ID号："+i+"的人购票结束");
    }

    @GetMapping("lock")
    public void sellTicketWithLock() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(5);
        //创建锁
        zkUtil.createLock();
        for (int i=0; i <5; i++){
            int finalI = i;
            new Thread(() -> {
                try {
                    String lock = zkUtil.createOwnLock();
                    System.out.println("购票人创建节点："+lock);
                    //获取锁
                    zkUtil.getLock(lock);
                    sell(finalI);
                    //释放锁
                    zkUtil.destoryLock(lock);
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    countDownLatch.countDown();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println("购票结束");
    }
}
