package com.atguigu;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.util.DimUtil;
import com.atguigu.gmall.util.DruidPhoenixDSUtil;

import java.sql.SQLException;
import java.util.List;

/**
 * @author yhm
 * @create 2022-08-29 9:19
 */
public class Test11_Time {
    public static void main(String[] args) throws SQLException {
        DruidDataSource dataSource = DruidPhoenixDSUtil.getDataSource();

        long start = System.currentTimeMillis();

        JSONObject dimSkuInfo = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_SPU_INFO", "1");
        System.out.println(dimSkuInfo);
        long end = System.currentTimeMillis();
        JSONObject dimSkuInfo1 = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_BASE_TRADEMARK", "1");
        System.out.println(dimSkuInfo1);
        long end1 = System.currentTimeMillis();
        JSONObject dimSkuInfo2 = DimUtil.getDimInfo(dataSource.getConnection(), "DIM_BASE_CATEGORY1", "1");
        System.out.println(dimSkuInfo2);
        long end2 = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println(end1 - end);
        System.out.println(end2 - end1);
    }
}
