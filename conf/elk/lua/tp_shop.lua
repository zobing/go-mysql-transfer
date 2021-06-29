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

-- 热度：heats
result["sh_heats"] = row["sales_volume"] * 3 + row["scan"] + row["popularity_bonus"]

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
-- 当前站点是否可见：is_show
result["lp_is_show"] = 1

-- 套餐状态 0-下架 1-上架 2-售罄：type
result["lp_type"] = 1

-- 是否需要填写配送地址：booking_show_address
result["lp_booking_show_address"] = 0

-- 渠道库存：channel_stock
result["lp_channel_stock"] = 100000

-- 有效开始时间：valid_begin_date
result["lp_valid_begin_date"] = 0

-- 有效结束时间：valid_end_date
result["lp_valid_end_date"] = 4102416000000

-- 外拓状态：0下架；1上架 id：status
result["lp_status"] = 1

-- 外拓利润利大于3%可售
result["lp_can_sale"] = 1

-- 渠道库存是否满足单次购买数量
result["lp_is_meet_with_single_min"] = 1

result['lp_lian_id'] = string.format("shop_%d", row["shop_id"])
--[[ 联联商品信息 结束 --]]

local currentTime = os.date("%Y-%m-%d %H:%M:%S")

-- 操作ES
if action == "insert" then -- 新增事件
    -- 新增，参数1为index名称，string类型；参数2为要插入的数据主键；参数3为要插入的数据，tablele类型或者json字符串
    result['created_at'] = currentTime
    result['updated_at'] = currentTime
    ops.INSERT("jyjzzk_shop_product", id, result)
elseif action == "delete" then -- 删除事件
    -- 删除，参数1为index名称，string类型；参数2为要插入的数据主键
    ops.DELETE("jyjzzk_shop_product", id)
else -- 修改事件
    -- 修改，参数1为index名称，string类型；参数2为要插入的数据主键；参数3为要插入的数据，tablele类型或者json字符串
    result['updated_at'] = currentTime
    ops.UPDATE("jyjzzk_shop_product", id, result)
end
