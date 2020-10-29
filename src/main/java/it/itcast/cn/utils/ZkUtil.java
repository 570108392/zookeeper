package it.itcast.cn.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @createUser: 张鹏
 * @createTime: 2020/10/29
 * @descripton: zookeeper 公共类通用方法
 **/
@Component
@Slf4j
public class ZkUtil {
    @Autowired
    private  ZooKeeper zkClient;


    /**
     * 同步方法
     * 判断节点是否存在
     * @param path 节点名称
     * @param watch 是否启用监听
     * @return
     */
    public Stat exists(String path,Boolean watch){
        try {
            Stat stat = zkClient.exists(path, watch);
            if (null == stat)
                return null;
            return stat;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("判断获取{}节点是否存在存在异常",path);
            return null;
        }
    }

    /**
     * 同步方法
     * 判断节点是否存在，并设置监控： 创建 删除 修改
     * @param path 节点名称
     * @param watcher 监听事件
     * @return
     */
    public Boolean exists(String path, Watcher watcher){
        try {
            Stat stat = zkClient.exists(path, watcher);
            if (null == stat) {
                log.error("{}节点不存在",path);
                return false;
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("判断获取{}节点是否存在存在异常",path);
            return false;
        }
    }

    /**
     * 创建节点
     * @param path 节点名称
     * @param data 节点携带内容 二进制
     * @param acl  节点权限
     * @param createMode 节点持久化类型 {持久化节点，持久化有序节点，临时节点，临时有序几点}
     * @return
     */
    public String createNode(String path, String data, List<ACL> acl, CreateMode createMode){
        try {
            return zkClient.create(path, data.getBytes(), acl, createMode);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("创建{}节点存在异常",path);
            return null;
        }
    }

    /**
     * 更新节点内容
     * @param path 节点名称
     * @param data 节点内容
     * @return
     */
    public Boolean updateNode(String path,String data){
        try {
            zkClient.setData(path,data.getBytes(),-1);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("更新{}节点存在异常",path);
            return false;
        }
    }

    /**
     * 删除节点
     * @param path 节点名称
     * @return
     */
    public Boolean deleteNode(String path){
        try {
            zkClient.delete(path,-1);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("删除{}节点存在异常",path);
            return false;
        }
    }

    /**
     * 获取当前节点的子节点(不包含孙子节点)
     * @param path 父节点path
     */
    public List<String> getChildren(String path){
        List<String> list = null;
        try {
            list = zkClient.getChildren(path, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 获取指定节点的值
     * @param path
     * @return
     */
    public  String getData(String path,Watcher watcher){
        try {
            Stat stat=new Stat();
            byte[] bytes=zkClient.getData(path,watcher,stat);
            return  new String(bytes);
        }catch (Exception e){
            e.printStackTrace();
            return  null;
        }
    }

    /**
     * 获取分布式唯一ID
     * @return
     */
    public String getId(){
        try {
            //新增节点
            String id = zkClient.create("/uuid",new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            return id.substring(5);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * 创建锁
     * @return
     */
    public void createLock(){

        Stat stat = exists("/lock", false);
        if (null == stat)
            createNode("/lock","lock",ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

    }

    /**
     * 创建锁
     * @return
     */
    public String createOwnLock(){


        String node = createNode("/lock/lock", "/lock/lock", ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        exists(node,watcher);
        return node;
    }

    /**
     * 获取锁
     * @return
     */
    public void getLock(String path) throws InterruptedException {

        //2 获取所有的锁
        System.out.println("购票人自己所节点"+path.substring(6));
        List<String> children = getChildren("/lock");
        //3 根据所排序 判断自己的锁是否在首位
        Collections.sort(children);
        System.out.println("获取所有节点"+children.toString());
        //4 在首位获取锁
        int i = children.indexOf(path.substring(6));
        if (0 == i)
            return ;
        else{
            Stat stat = exists("/lock/"+children.get(i-1),false);
            if (null == stat){
                getLock(path);
            }else{
                synchronized (watcher){
                    watcher.wait();
                }
            }
        }
    }

    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
            if (Event.EventType.NodeDeleted == watchedEvent.getType()){
                synchronized (this){
                    watcher.notifyAll();
                }
            }
        }
    };
    /**
     * 释放锁
     * @return
     */
    public Boolean destoryLock(String path){
        System.out.println("删除节点："+path);
        return deleteNode(path);
    }

}
