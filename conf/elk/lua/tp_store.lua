-- 在线执行lua脚本 https://www.runoob.com/try/runcode.php?filename=datatype1&type=lua

local ops = require("esOps") --加载elasticsearch操作模块
local db = require("dbOps") --加载数据库操作模块

local row = ops.rawRow()  --当前数据库的一行数据,table类型，key为列名称
local action = ops.rawAction()  --当前数据库事件,包括：insert、update、delete

-- 定义一个table,作为结果集
local result = {}

-- 拼接索引
local id

-- 查询商户下所有门店信息, 数据库名必需
local shopSql = string.format("SELECT * FROM wtshop.tp_shop WHERE store_id = %d", row["store_id"])
local shopList = db.select(shopSql)

for i, shopInfo in ipairs(shopList) do
    if next(shopInfo) ~= nil then
        -- 索引
        if shopInfo["shop_id"] ~= nil then
            id = string.format("shop_%d", shopInfo["shop_id"])
        else
            id = 'shop_0'
        end

        -- 店铺名称：store_name
        if row["store_name"] ~= nil then
            result["st_store_name"] = row["store_name"]
        else
            result["st_store_name"] = ""
        end

        -- 店铺分类id：sc_id
        if row["sc_id"] ~= nil then
            result["st_sc_id"] = row["sc_id"]
        else
            result["st_sc_id"] = 0
        end

        -- 店铺状态，0关闭，1开启，2审核中：store_state
        if row["store_state"] ~= nil then
            result["st_store_state"] = row["store_state"]
        else
            result["st_store_state"] = 2
        end

        -- 未删除0，已删除1：deleted
        if row["deleted"] ~= nil then
            result["st_deleted"] = row["deleted"]
        else
            result["st_deleted"] = 0
        end

        -- 是否整合至外拓商圈：0否；1是，is_show_wt
        if row["is_show_wt"] ~= nil then
            result["st_is_show_wt"] = row["is_show_wt"]
        else
            result["st_is_show_wt"] = 0
        end

        -- 操作ES
        if action == "update" then -- 更新事件
            -- 修改，参数1为index名称，string类型；参数2为要插入的数据主键；参数3为要插入的数据，tablele类型或者json字符串
            ops.UPDATE("jyjzzk_shop_product", id, result)
        end
    end
end
