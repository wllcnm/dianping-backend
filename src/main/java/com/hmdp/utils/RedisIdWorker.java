package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    /*
     * 开始时间戳
     * */

    public static final long BEGIN_TIMESTAMP = 1675467480L;

    public static final long COUNT_BITS=32;
    @Resource
    private  StringRedisTemplate stringRedisTemplate;

    public  long nextId(String keyPrefix) {
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        //2.生成序列号
        //3.生成当天日期
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //4.拼接key
        Long increment = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        //5.返回
        return timestamp<<COUNT_BITS | increment;
    }


}
