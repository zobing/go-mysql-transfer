-- 在线执行lua脚本 https://www.runoob.com/try/runcode.php?filename=datatype1&type=lua

local ops = require("esOps") --加载elasticsearch操作模块
local db = require("dbOps") --加载数据库操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、update、delete

-- 定义一个table,作为结果集
local result = {}

-- 拼接索引
local id
if row["id"] ~= nil then
    id = string.format("lian_product_%d", row["id"])
else
    id = 'lian_product_0'
end
result["id"] = id

--[[ 商品信息 开始 --]]
-- 联联产品索引ID：id
if row["id"] ~= nil then
    result["lp_id"] = row["id"]
else
    result["lp_id"] = 0
end

-- 联联产品ID：lian_id
if row["lian_id"] ~= nil then
    result["lp_lian_id"] = row["lian_id"]
else
    result["lp_lian_id"] = 0
end

-- 联联站点ID | tp_lian_location.lian_id：location_id
if row["location_id"] ~= nil then
    result["lp_location_id"] = row["location_id"]
else
    result["lp_location_id"] = 0
end

-- 产品名称：only_name
if row["only_name"] ~= nil then
    result["lp_only_name"] = row["only_name"]
else
    result["lp_only_name"] = ""
end

-- 产品标题：title
if row["title"] ~= nil then
    result["lp_title"] = row["title"]
else
    result["lp_title"] = ""
end

-- 产品标题（展示前端）：product_title
if row["product_title"] ~= nil then
    result["lp_product_title"] = row["product_title"]
else
    result["lp_product_title"] = ""
end

-- 产品简称(分享文字介绍)：share_text
if row["share_text"] ~= nil then
    result["lp_share_text"] = row["share_text"]
else
    result["lp_share_text"] = ""
end

-- 封面图片：face_img
if row["face_img"] ~= nil then
    result["lp_face_img"] = row["face_img"]
else
    result["lp_face_img"] = ""
end

-- 商家地址：address
if row["address"] ~= nil then
    result["lp_address"] = row["address"]
else
    result["lp_address"] = ""
end

-- 商家电话：tel
if row["tel"] ~= nil then
    result["lp_tel"] = row["tel"]
else
    result["lp_tel"] = ""
end

-- 抢购结束时间：end_time
if row["end_time"] ~= nil then
    result["lp_end_time"] = row["end_time"]
else
    result["lp_end_time"] = 0
end

-- 抢购开始时间：begin_time
if row["begin_time"] ~= nil then
    result["lp_begin_time"] = row["begin_time"]
else
    result["lp_begin_time"] = 0
end

-- 有效结束时间：valid_end_date
if row["valid_end_date"] ~= nil then
    result["lp_valid_end_date"] = row["valid_end_date"]
else
    result["lp_valid_end_date"] = 0
end

-- 有效开始时间：valid_begin_date
if row["valid_begin_date"] ~= nil then
    result["lp_valid_begin_date"] = row["valid_begin_date"]
else
    result["lp_valid_begin_date"] = 0
end

-- 渠道库存：channel_stock
if row["channel_stock"] ~= nil then
    result["lp_channel_stock"] = row["channel_stock"]
else
    result["lp_channel_stock"] = 0
end

-- 渠道销量：channel_sale_amount
if row["channel_sale_amount"] ~= nil then
    result["lp_channel_sale_amount"] = row["channel_sale_amount"]
else
    result["lp_channel_sale_amount"] = 0
end

-- 渠道销量：channel_sale_amount
if row["channel_sale_amount"] ~= nil then
    result["lp_channel_sale_amount"] = row["channel_sale_amount"]
else
    result["lp_channel_sale_amount"] = 0
end

-- 产品分类全路径,使用’-\r\n’分隔子级与父级,父级在前：category_path
if row["category_path"] ~= nil then
    result["lp_category_path"] = row["category_path"]
else
    result["lp_category_path"] = ""
end

-- 产品分类 id：product_category_id
if row["product_category_id"] ~= nil then
    result["lp_product_category_id"] = row["product_category_id"]
else
    result["lp_product_category_id"] = 0
end

-- 外拓状态：0下架；1上架 id：status
if row["status"] ~= nil then
    result["lp_status"] = row["status"]
else
    result["lp_status"] = 0
end

-- 城市编码：city_code
if row["city_code"] ~= nil then
    result["lp_city_code"] = row["city_code"]
    result["city_code"] = row["city_code"]
else
    result["lp_city_code"] = ""
    result["city_code"] = ""
end

-- 当前站点是否可见：is_show
if row["is_show"] ~= nil then
    result["lp_is_show"] = row["is_show"]
else
    result["lp_is_show"] = 0
end

-- 套餐状态 0-下架 1-上架 2-售罄：type
if row["type"] ~= nil then
    result["lp_type"] = row["type"]
else
    result["lp_type"] = -1
end

-- 是否需要填写配送地址：booking_show_address
if row["booking_show_address"] ~= nil then
    result["lp_booking_show_address"] = row["booking_show_address"]
else
    result["lp_booking_show_address"] = 0
end

-- 预约方式 0-无需预约 1-网址预约 2-电话预约：booking_type
if row["booking_type"] ~= nil then
    result["lp_booking_type"] = row["booking_type"]
else
    result["lp_booking_type"] = 0
end

-- （最低套餐售价的套餐）售价（分）：sale_price
if row["sale_price"] ~= nil then
    result["lp_sale_price"] = row["sale_price"]
    -- 最低利润
    result["lp_min_profit"] = row["sale_price"] * 0.03
else
    result["lp_sale_price"] = 0
    result["lp_min_profit"] = 0
end

-- （最低套餐售价的套餐）结算价（分）：channel_price
if row["channel_price"] ~= nil then
    result["lp_channel_price"] = row["channel_price"]
else
    result["lp_channel_price"] = 0
end

-- （最低套餐售价的套餐）原价（分）：origin_price
if row["origin_price"] ~= nil then
    result["lp_origin_price"] = row["origin_price"]
else
    result["lp_origin_price"] = 0
end

-- 利润
if (row["sale_price"] ~= nil) and (row["channel_price"] ~= nil) then
    result["lp_profit"] = row["sale_price"] - row["channel_price"]
else
    result["lp_profit"] = 0
end

-- 是否可以销售
if result["lp_profit"] > result["lp_min_profit"] then
    result['lp_can_sale'] = 1
else
    result['lp_can_sale'] = 0
end

-- 渠道库存是否满足单次购买数量
if row["channel_stock"] >= row["single_min"] then
    result['lp_is_meet_with_single_min'] = 1
else
    result['lp_is_meet_with_single_min'] = 0
end

-- 经度
if (row["longitude"] ~= nil) and (row["longitude"] ~= "") then
    result["longitude"] = row["longitude"]
else
    result["longitude"] = 0
end

-- 纬度
if (row["latitude"] ~= nil) and (row["latitude"] ~= "") then
    result["latitude"] = row["latitude"]
else
    result["latitude"] = 0
end

-- 坐标
if (row["longitude"] ~= nil) and (row["latitude"] ~= nil) and (row["longitude"] ~= "") and (row["latitude"] ~= "") then
    local lng = tonumber(row["longitude"])
    local lat = tonumber(row["latitude"])
    if (lng >= -180) and (lng <= 180) and (lat >= -90) and (lat <= 90) then
        result["coordinate"] = {lng, lat}
    else
        result["coordinate"] = {0, 0}
    end
else
    result["coordinate"] = {0, 0}
end
--[[ 商品信息 结束 --]]

-- 数据类型
result["source_type"] = "lian_product"

-- 数据来源
result["source"] = "binlog"

--[[ 门店信息 开始 --]]
-- 门店删除状态
result["sh_deleted"] = 0

-- 门店开启状态
result["sh_shop_status"] = 1

-- 是否整合到外拓商圈
result["st_is_show_wt"] = 1

-- 商户开启状态
result["st_store_state"] = 1

-- 商户删除状态
result["st_deleted"] = 0
--[[ 门店信息 结束 --]]

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
