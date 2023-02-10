package com.hmdp.utils;

public interface ILock {
    /*
     * @return true:代表获取锁成功,false获取锁失败
     * */
    boolean tryLock(long timeoutSec);

    void unlock();
}
