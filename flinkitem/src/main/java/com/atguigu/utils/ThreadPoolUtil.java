package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {

    private static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            5, TimeUnit.MINUTES,
                            new LinkedBlockingQueue<>());
                }
            }
        }

        return threadPoolExecutor;
    }

    public static void main(String[] args) {

        ThreadPoolExecutor threadPoolExecutor1 = getThreadPoolExecutor();

        for (int i = 0; i < 10; i++) {
            threadPoolExecutor1.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println(Thread.currentThread().getName());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

    }
}
