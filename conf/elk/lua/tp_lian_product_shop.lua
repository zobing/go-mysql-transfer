-- 在线执行lua脚本 https://www.runoob.com/try/runcode.php?filename=datatype1&type=lua

local ops = require("esOps") --加载elasticsearch操作模块
local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、update、delete

-- 定义一个table,作为结果集
local result = {}

-- 联联商家ID：lian_id
if row["lian_id"] ~= nil then
    result["lian_id"] = row["lian_id"]
else
    result["lian_id"] = 0
end

-- 联联产品ID（tp_lian_product.lian_id）：lian_product_id
if row["lian_product_id"] ~= nil then
    result["lian_product_id"] = row["lian_product_id"]
else
    result["lian_product_id"] = 0
end

-- 商家名称：name
if row["name"] ~= nil then
    result["name"] = row["name"]
else
    result["name"] = ""
end

-- 商家地址：address
if row["address"] ~= nil then
    result["address"] = row["address"]
else
    result["address"] = ""
end

-- 商家电话：phone_number
if row["phone_number"] ~= nil then
    result["phone_number"] = row["phone_number"]
else
    result["phone_number"] = ""
end

-- 纬度：latitude
if row["latitude"] ~= nil then
    result["latitude"] = row["latitude"]
else
    result["latitude"] = 0
end

-- 经度：longitude
if row["longitude"] ~= nil then
    result["longitude"] = row["longitude"]
else
    result["longitude"] = 0
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

local currentTime = os.date("%Y-%m-%d %H:%M:%S")

-- 操作ES
if action == "insert" then -- 新增事件
    -- 新增，参数1为index名称，string类型；参数2为要插入的数据主键；参数3为要插入的数据，tablele类型或者json字符串
    result['created_at'] = currentTime
    result['updated_at'] = currentTime
    ops.INSERT("jyjzzk_lian_product_shop", result["lian_id"], result)
elseif action == "delete" then -- 删除事件
    -- 删除，参数1为index名称，string类型；参数2为要插入的数据主键
    ops.DELETE("jyjzzk_lian_product_shop", result["lian_id"])
else -- 修改事件
    -- 修改，参数1为index名称，string类型；参数2为要插入的数据主键；参数3为要插入的数据，tablele类型或者json字符串
    result['updated_at'] = currentTime
    ops.UPDATE("jyjzzk_lian_product_shop", result["lian_id"], result)
end
