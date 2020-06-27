'use strict'
const KOCString = require('koc-common-string')
const { Sequelize, DataTypes, Op } = require('sequelize')

const clients = {}
let cacheRedis = null

const KOCSequelize = module.exports = {
  /**
   * @description 初始化数据库连接
   * @param {Object[]} dbList 数据库连接配置对象数组
   * @param {Object} dbList.config 数据库连接配置对象
   * @param {string} dbList.config.host 地址
   * @param {number} dbList.config.port 端口
   * @param {string} dbList.config.username 账号
   * @param {string} dbList.config.password 密码
   * @param {string} dbList.config.database 数据库
   * @param {string} [dbList.config.dialect] 数据库类型
   * @param {boolean} [dbList.config.logging] 是否打印log true false
   * @param {Object} [dbList.config.pool] 连接池配置
   * @param {number} [dbList.config.pool.max] 连接池最大数量
   * @param {number} [dbList.config.pool.idle] 连接池最大数量
   * @param {number} [dbList.config.pool.acquire] 连接池最大数量
   * @param {Object} [dbList.config.define] model配置
   * @param {string} [dbList.config.define.underscored] 是否是下划线定义模型名称
   * @param {string} [dbList.config.define.freezeTableName] 是否冻结表名称
   * @param {string} [dbList.config.define.charset] 字符集
   * @param {Object} [dbList.config.define.dialectOptions]
   * @param {string} [dbList.config.define.dialectOptions.collate]
   * @param {boolean} [dbList.config.define.timestamps] 是否启用自动创建字段craetAt updateAt
   * @retun {Object} Sequelize 数据库连接实例
   * @param {Object} redis Redis数据库连接实例
   * @returns {Object} 数据库连接对象
   */
  Init: (dbList, redis) => {
    dbList = KOCString.ToArray(dbList)
    for (const thisValue of dbList) {
      if (thisValue.logging) thisValue.logging = console.log
      clients[thisValue.name] = new Sequelize(thisValue)
    }
    cacheRedis = redis
    return clients
  },
  /**
   * @description 缓存过期时间(分钟)默认3分钟
   * @param {number} expire 过期时间(分钟)
   * @returns {number}
   */
  CacheExpire: (expire) => KOCString.ToIntPositive(expire, 3) * 60,
  /**
   * @description 缓存Key
   * @param {string} dbname 数据库名称
   * @param {Object} parm 参数对象
   * @returns {string}
   */
  CacheKey: (dbname, parm) => KOCString.MD5(KOCString.ToString(dbname) + JSON.stringify(parm)),
  /**
   * @description 缓存写入
   * @param {string} dbname 数据库名称
   * @param {Object} parm 参数对象
   * @param {Object} object 数据对象
   * @param {number} expire 过期秒数
   */
  CachePut: (dbname, parm, object, expire) => {
    if (!cacheRedis || !object) return
    cacheRedis.set(KOCSequelize.CacheKey(dbname, parm), JSON.stringify(object), 'EX', KOCSequelize.CacheExpire(expire))
  },
  DataTypes,
  Op
}
