package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public List<ShopType> queryShopType() {
        //1.从redis获取缓存
        String jsonStr = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        //2.判断缓存是否存在
        if (StrUtil.isNotBlank(jsonStr)) {
            //3.存在,自己返回
            return JSONUtil.toList(jsonStr, ShopType.class);
        }
        //4.不存在,从数据库中查询
        List<ShopType> list = query().select("id", "name", "icon", "sort").list();
        //5.将list转换为json
        String s = JSONUtil.toJsonStr(list);
        //6.将数据存入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, s);
        //7.返回
        return list;
    }
}
