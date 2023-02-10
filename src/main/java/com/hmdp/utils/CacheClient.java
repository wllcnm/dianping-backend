package com.hmdp.utils;

import cn.hutool.core.lang.TypeReference;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;

@Slf4j
@Component
public class CacheClient {
    public static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    private final StringRedisTemplate stringRedisTemplate;


    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    //设置逻辑过期
    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        //写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //解决缓存击穿,通过逻辑过期
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        //1.从redis中查询商铺缓存
        String key = keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //3.不存在,直接返回
            return null;
        }
        //命中
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())) {
            //未过期
            return r;
        }
        //已过期
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        //判断锁是否获取成功
        if (isLock) {

            //获取锁成功,调用其他线程执行重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });

        }
        //如果加锁的话就,返回过期的商铺信息
        return r;
    }

    //解决缓存穿透
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        //1.从redis中查询商铺缓存
        String key = keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在,直接返回
            return JSONUtil.toBean(shopJson, type);
        }
        //判断命中的是否是空值
        if (Objects.equals(shopJson, "")) {
            //返回错误信息
            return null;
        }
        //4.不存在,根据id查询数据库
        R r = dbFallback.apply(id);
        //5.数据库中不存在,返回错误
        if (r == null) {
            stringRedisTemplate.opsForValue().set(key, "", time, unit);
            return null;
        }
        //6.数据库查询完后,向redis写入
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r), time, unit);
        return r;
    }

    //解决缓存击穿,通过互斥锁
    public <R, ID> R queryWithMutex(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        //1.从redis中查询商铺缓存
        String key = keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //3.存在,直接返回
            R r = JSONUtil.toBean(shopJson, type);
            return r;
        }
        //判断命中的是否是空值
        if (Objects.equals(shopJson, "")) {
            //返回错误信息
            return null;
        }
        //4.实现缓存重建
        //4.1获取互斥锁
        String lockKey = "lock:shop:" + id;
        R r1 = null;
        try {
            boolean islock = tryLock(lockKey);
            //4.2判断是否获取成功
            if (!islock) {
                //4.3失败,休眠重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }

            //4.4 获取锁成功,根据id查询数据库
            r1 = dbFallback.apply(id);
            //模拟重建延迟
            Thread.sleep(200);
            //5.数据库中不存在,返回错误
            if (r1 == null) {
                stringRedisTemplate.opsForValue().set(key, "", time, unit);
                return null;
            }
            //6.数据库查询完后,向redis写入
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(r1), time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7.释放互斥锁
            unlock(lockKey);
        }
        return r1;
    }

    //利用redis的setnx实现加锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //利用redis的del实现解锁
    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
