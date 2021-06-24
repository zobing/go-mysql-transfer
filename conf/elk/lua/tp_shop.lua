-- 在线执行lua脚本 https://www.runoob.com/try/runcode.php?filename=datatype1&type=lua

local ops = require("esOps") --加载elasticsearch操作模块
local db = require("dbOps") --加载数据库操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、update、delete

-- 定义一个table,作为结果集
local result = {}

-- 拼接索引
local id
if row["shop_id"] ~= nil then
    id = string.format("shop_%d", row["shop_id"])
else
    id = 'shop_0'
end
result["id"] = id

-- 查询门店对应的商户信息, 数据库名必需
local storeSql = string.format("SELECT * FROM esshop.tp_store WHERE store_id = %d", row["store_id"])
local storeInfo = db.selectOne(storeSql)
if next(storeInfo) ~= nil then
    -- 店铺索引id
    if storeInfo["store_id"] ~= nil then
        result["st_store_id"] = storeInfo["store_id"]
    else
        result["st_store_id"] = 0
    end

    -- 店铺名称：store_name
    if storeInfo["store_name"] ~= nil then
        result["st_store_name"] = storeInfo["store_name"]
    else
        result["st_store_name"] = ""
    end

    -- 店铺商圈id：bid
    if storeInfo["bid"] ~= nil then
        result["st_bid"] = storeInfo["bid"]
    else
        result["st_bid"] = 0
    end

    -- 店铺分类id：sc_id
    if storeInfo["sc_id"] ~= nil then
        result["st_sc_id"] = storeInfo["sc_id"]
    else
        result["st_sc_id"] = 0
    end

    -- 店铺状态，0关闭，1开启，2审核中：store_state
    if storeInfo["store_state"] ~= nil then
        result["st_store_state"] = storeInfo["store_state"]
    else
        result["st_store_state"] = 2
    end

    -- 未删除0，已删除1：deleted
    if storeInfo["deleted"] ~= nil then
        result["st_deleted"] = storeInfo["deleted"]
    else
        result["st_deleted"] = 0
    end

    -- 是否整合至外拓商圈：0否；1是，is_show_wt
    if storeInfo["is_show_wt"] ~= nil then
        result["st_is_show_wt"] = storeInfo["is_show_wt"]
    else
        result["st_is_show_wt"] = 0
    end
else
    result["st_store_id"] = 0
    result["st_store_name"] = ""
    result["st_bid"] = 0
    result["st_sc_id"] = 0
    result["st_store_state"] = 2
    result["st_deleted"] = 0
    result["st_is_show_wt"] = 0
end

-- 查询门店所在城市编码信息, 数据库名必需
local regionSql = string.format("SELECT * FROM esshop.tp_region WHERE id = %d", row["city_id"])
local regionInfo = db.selectOne(regionSql)
if next(regionInfo) ~= nil then
    result["city_code"] = regionInfo['code']
else
    result["city_code"] = ""
end

--[[ 门店信息 开始 --]]
-- 门店索引id：shop_id
if row["shop_id"] ~= nil then
    result["sh_shop_id"] = row["shop_id"]
else
    result["sh_shop_id"] = 0
end

-- 商家ID：store_id
if row["store_id"] ~= nil then
    result["sh_store_id"] = row["store_id"]
else
    result["sh_store_id"] = 0
end

-- 商圈id：bid
if row["bid"] ~= nil then
    result["sh_bid"] = row["bid"]
else
    result["sh_bid"] = 0
end

-- 门店名称：shop_name
if row["shop_name"] ~= nil then
    result["sh_shop_name"] = row["shop_name"]
else
    result["sh_shop_name"] = ""
end

-- 门店所在省份ID：province_id
if row["province_id"] ~= nil then
    result["sh_province_id"] = row["province_id"]
else
    result["sh_province_id"] = 0
end

-- 门店所在城市ID：city_id
if row["city_id"] ~= nil then
    result["sh_city_id"] = row["city_id"]
else
    result["sh_city_id"] = 0
end

-- 门店所在地区ID：district_id
if row["district_id"] ~= nil then
    result["sh_district_id"] = row["district_id"]
else
    result["sh_district_id"] = 0
end

-- 详细地址：shop_address
if row["shop_address"] ~= nil then
    result["sh_shop_address"] = row["shop_address"]
else
    result["sh_shop_address"] = ""
end

-- 门店地址经度（百度坐标）：longitude
if row["longitude"] ~= nil then
    result["sh_longitude"] = row["longitude"]
else
    result["sh_longitude"] = 0
end

-- 门店地址纬度（百度坐标）：latitude
if row["latitude"] ~= nil then
    result["sh_latitude"] = row["latitude"]
else
    result["sh_latitude"] = 0
end

-- 门店地址经度（腾讯坐标）：longitude_tencent
if row["longitude_tencent"] ~= nil then
    result["sh_longitude_tencent"] = row["longitude_tencent"]
else
    result["sh_longitude_tencent"] = 0
end

-- 门店地址纬度（腾讯坐标）：latitude_tencent
if row["latitude_tencent"] ~= nil then
    result["sh_latitude_tencent"] = row["latitude_tencent"]
else
    result["sh_latitude_tencent"] = 0
end

-- 门店状态，0关闭，1开启，2审核中：shop_status
if row["shop_status"] ~= nil then
    result["sh_shop_status"] = row["shop_status"]
else
    result["sh_shop_status"] = 0
end

-- 未删除0，已删除1：deleted
if row["deleted"] ~= nil then
    result["sh_deleted"] = row["deleted"]
else
    result["sh_deleted"] = 0
end

-- 门店简介：shop_intro
if row["shop_intro"] ~= nil then
    result["sh_shop_intro"] = row["shop_intro"]
else
    result["sh_shop_intro"] = ""
end

-- 门店logo：shop_logo
if row["shop_logo"] ~= nil then
    result["sh_shop_logo"] = row["shop_logo"]
else
    result["sh_shop_logo"] = ""
end

-- 人均消费：average_consumption
if row["average_consumption"] ~= nil then
    result["sh_average_consumption"] = row["average_consumption"]
else
    result["sh_average_consumption"] = 0
end

-- 买单优惠说明文字：preferential_description
if row["preferential_description"] ~= nil then
    result["sh_preferential_description"] = row["preferential_description"]
else
    result["sh_preferential_description"] = ""
end

-- 人气值加成：popularity_bonus
if row["popularity_bonus"] ~= nil then
    result["sh_popularity_bonus"] = row["popularity_bonus"]
else
    result["sh_popularity_bonus"] = 0
end

-- 销量：sales_volume
if row["sales_volume"] ~= nil then
    result["sh_sales_volume"] = row["sales_volume"]
else
    result["sh_sales_volume"] = 0
end

-- 排序：recommend_sort
if row["recommend_sort"] ~= nil then
    result["sh_recommend_sort"] = row["recommend_sort"]
else
    result["sh_recommend_sort"] = 0
end

-- 0是不推荐，1是推荐：is_recommend
if row["is_recommend"] ~= nil then
    result["sh_is_recommend"] = row["is_recommend"]
else
    result["sh_is_recommend"] = 0
end

-- 虚拟分享次数：virtual_share_times
if row["virtual_share_times"] ~= nil then
    result["sh_virtual_share_times"] = row["virtual_share_times"]
else
    result["sh_virtual_share_times"] = 0
end

-- 虚拟分享次数：virtual_share_times
if row["virtual_share_times"] ~= nil then
    result["sh_virtual_share_times"] = row["virtual_share_times"]
else
    result["sh_virtual_share_times"] = 0
end

-- 支付结算中心门店ID：pay_sid
if row["pay_sid"] ~= nil then
    result["sh_pay_sid"] = row["pay_sid"]
else
    result["sh_pay_sid"] = 0
end

-- 坐标
if (row["longitude_tencent"] ~= nil) and (row["latitude_tencent"] ~= nil) then
    if (row["longitude_tencent"] >= -180) and (row["longitude_tencent"] <= 180) and (row["latitude_tencent"] >= -90) and (row["latitude_tencent"] <= 90) then
        result["coordinate"] = {row["longitude_tencent"], row["latitude_tencent"]}
    else
        result["coordinate"] = {0, 0}
    end
else
    result["coordinate"] = {0, 0}
end
--[[ 门店信息 结束 --]]

-- 数据类型
result["source_type"] = "shop"

-- 数据来源
result["source"] = "binlog"

--[[ 联联商品信息 开始 --]]
--[[
-- 联联产品索引ID：id
result["lp_id"] = 0

-- 联联产品ID：lian_id
result["lp_lian_id"] = 0

-- 联联站点ID | tp_lian_location.lian_id：location_id
result["lp_location_id"] = 0

-- 产品名称：only_name
result["lp_only_name"] = ""

-- 产品标题：title
result["lp_title"] = ""

-- 产品标题（展示前端）：product_title
result["lp_product_title"] = ""

-- 产品简称(分享文字介绍)：share_text
result["lp_share_text"] = ""

-- 封面图片：face_img
result["lp_face_img"] = ""

-- 商家地址：address
result["lp_address"] = ""

-- 商家电话：tel
result["lp_tel"] = ""

-- 抢购结束时间：end_time
result["lp_end_time"] = 0

-- 抢购开始时间：begin_time
result["lp_begin_time"] = 0

-- 有效结束时间：valid_end_date
result["lp_valid_end_date"] = 0

-- 有效开始时间：valid_begin_date
result["valid_begin_date"] = 0

-- 渠道库存：channel_stock
result["lp_channel_stock"] = 0

-- 渠道销量：channel_sale_amount
result["lp_channel_sale_amount"] = 0

-- 渠道销量：channel_sale_amount
result["lp_channel_sale_amount"] = 0

-- 产品分类全路径,使用’-\r\n’分隔子级与父级,父级在前：category_path
result["lp_category_path"] = ""

-- 产品分类 id：product_category_id
result["lp_product_category_id"] = 0

-- 外拓状态：0下架；1上架 id：status
result["lp_status"] = 0

-- 城市编码：city_code
result["lp_city_code"] = ""

-- 当前站点是否可见：is_show
result["lp_is_show"] = 0

-- 套餐状态 0-下架 1-上架 2-售罄：type
result["lp_type"] = -1

-- 是否需要填写配送地址：booking_show_address
result["lp_booking_show_address"] = 0

-- 预约方式 0-无需预约 1-网址预约 2-电话预约：booking_type
result["lp_booking_type"] = 0

-- （最低套餐售价的套餐）售价（分）：sale_price
result["lp_sale_price"] = 0
result["lp_min_profit"] = 0

-- （最低套餐售价的套餐）结算价（分）：channel_price
result["lp_channel_price"] = 0

-- （最低套餐售价的套餐）原价（分）：origin_price
result["lp_origin_price"] = 0

-- 利润
result["lp_profit"] = 0
--]]
--[[ 联联商品信息 结束 --]]

-- 操作ES
if action == "insert" then -- 新增事件
    -- 新增，参数1为index名称，string类型；参数2为要插入的数据主键；参数3为要插入的数据，tablele类型或者json字符串
    ops.INSERT("jyjzzk_shop_product", id, result)
elseif action == "delete" then -- 删除事件
    -- 删除，参数1为index名称，string类型；参数2为要插入的数据主键
    ops.DELETE("jyjzzk_shop_product", id)
else -- 修改事件
    -- 修改，参数1为index名称，string类型；参数2为要插入的数据主键；参数3为要插入的数据，tablele类型或者json字符串
    ops.UPDATE("jyjzzk_shop_product", id, result)
end
