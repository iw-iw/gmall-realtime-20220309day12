package com.atguigu;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.DimUtil;
import com.atguigu.gmall.util.DruidPhoenixDSUtil;
import com.atguigu.gmall.util.JedisUtil;
import redis.clients.jedis.Jedis;

import java.sql.SQLException;

/**
 * @author yhm
 * @create 2022-08-29 10:34
 */
public class Test12_RedisTime {
    public static void main(String[] args) throws SQLException {
        String key = "DIM:DIM_SKU_INFO:15";
        Jedis jedis = JedisUtil.getJedis();
        DruidDataSource dataSource = DruidPhoenixDSUtil.getDataSource();
        // 优先读取redis
        String value = jedis.get(key);
        long start = System.currentTimeMillis();
        // 如果redis有数据直接返回
        if (value != null) {
            System.out.println(value);
        } else {
            // 如果redis没有数据  读取phoenix 同时存储到redis
            JSONObject dimSkuInfo = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_SKU_INFO", "15");
            if (dimSkuInfo != null) {
                System.out.println(dimSkuInfo);
                jedis.setex(key, 3600 * 24, dimSkuInfo.toJSONString());
            }
            long end = System.currentTimeMillis();
            System.out.println(end - start);
            JSONObject dimSkuInfo1 = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_SKU_INFO", "15");
            if (dimSkuInfo1 != null) {
                System.out.println(dimSkuInfo1);
                jedis.setex(key, 3600 * 24, dimSkuInfo1.toJSONString());
            }
            long end1 = System.currentTimeMillis();
            System.out.println(end1 - end);
        }

        long end = System.currentTimeMillis();
        System.out.println(end - start);
    }
}
