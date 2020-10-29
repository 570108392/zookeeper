package it.itcast.cn.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @createUser: 张鹏
 * @createTime: 2020/9/3
 * @descripton:
 **/
public class FixedThreadPool {
    private static final ExecutorService pool = Executors.newFixedThreadPool(500);

    public static void submit(Runnable runnable){
        pool.submit(runnable);
    }
}
