package com.yonyou.cloud.mail;

import com.yonyou.cloud.mail.utils.ESRepository;

/**
 * Created by zengxs on 2017/8/29.
 */
public class Main {

    private static final Object lock = new Object();

    public static void main(String[] args) throws InterruptedException {
        System.out.println(args[0] + ":" + args[1] + ":" + args[2] + ":" + args[3]);
        new ESRepository(args[0], args[1], args[2], args[3]).getClient();
        synchronized (lock) {
            lock.wait();
        }
    }
}
