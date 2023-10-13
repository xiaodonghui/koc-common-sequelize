'use strict'

const KOCString = require('koc-common-string')
const KOCRedis = require('koc-common-redis')
const KOCReturn = require('koc-common-return/index')
const { Sequelize, Op, DataTypes } = require('sequelize')

class KOCMysql {
  /**
   *
   * @param {Object} options
   * @param {string} options.database
   * @param {string} options.username
   * @param {string} options.password
   * @param {string} options.host
   * @param {number} options.port
   * @param {string} options.dialect
   * @param {Object} [options.dialectOptions]
   * @param {string} [options.dialectOptions.flags]
   * @param {boolean|function} [options.logging]
   * @param {boolean} [options.logQueryParameters]
   * @param {Object} [options.pool] 连接池配置
   * @param {number} [options.pool.max] 连接池最大数量
   * @param {number} [options.pool.idle] 连接池最大数量
   * @param {number} [options.pool.acquire] 连接池最大数量
   * @param {Object} [options.define] model配置
   * @param {boolean} [options.define.underscored] 是否是下划线定义model名称
   * @param {boolean} [options.define.freezeTableName] 是否冻结表名称
   * @param {boolean} [options.define.timestamps] 是否启用自动创建字段 createdAt updatedAt
   * @param {Object|Array|string} [redis] 缓存数据库配置
   * @param {Object|Array|string} [redis.name] 缓存数据库名称
   */
  constructor (options, redis) {
    options = options || {}
    options.dialectOptions = options.dialectOptions || {}
    if (options.logging === true) {
      options.logging = console.log
      options.logQueryParameters = true
    }
    options.dialectOptions.flags = options.dialectOptions.flags ? options.dialectOptions.flags + ',FOUND_ROWS' : 'FOUND_ROWS'
    options.define = options.define || {}
    if (!options.define.hasOwnProperty('underscored')) options.define.underscored = false
    if (!options.define.hasOwnProperty('freezeTableName')) options.define.freezeTableName = true
    if (!options.define.hasOwnProperty('timestamps')) options.define.timestamps = false
    // mysql实例化
    this.sequelize = new Sequelize(options)
    // redis实例化
    try {
      if (redis) this.redisClient = KOCRedis.Init(redis)[redis.name]
    } catch (ex) {
      console.error('kocMysql init redis error', ex)
    }
  }

  /**
   * @desc 开始事务
   * @return {Promise<*>}
   */
  BeginTransaction () {
    return KOCReturn.Promise(() => this.sequelize.transaction())
  }

  /**
   * @desc 提交事务
   * @param transaction 事务
   * @return {Promise<*>}
   */
  CommitTransaction (transaction) {
    return KOCReturn.Promise(() => transaction.commit())
  }

  /**
   * @desc 回滚事务
   * @param transaction
   * @return {Promise<KOCReturn>}
   */
  RollbackTransaction (transaction) {
    return KOCReturn.Promise(() => transaction.rollback())
  }

  /**
   * @desc 模型
   * @param  model
   * @param  model.Name
   * @param  model.Model
   * @param  model.Options
   * @return Model
   */
  Model (model) {
    return this.sequelize.define(model.Name, model.Model, model.Options)
  }

  /**
   * @desc 写入数据
   * @param model
   * @param [options]
   * @param [options.transaction] 事务
   * @param {Array|Object} [cacheRemove]
   * @return {Promise<*>}
   */
  Insert (model, options, cacheRemove) {
    return KOCReturn.Promise(() => model.create(options))
  }

  /**
   * @desc 写入或者更新
   * @param model
   * @param values
   * @param [options]
   * @param [options.transaction] 事务
   * @param [options.fields]
   * @param {Array|Object} [cacheRemove]
   * @param cacheRemove.Model
   * @param cacheRemove.Parm
   * @return {Promise<*>}
   */
  InsertOrUpdate (model, values, options, cacheRemove) {
    this.CacheRemoveList(cacheRemove)
    return KOCReturn.Promise(() => model.upsert(values, options))
  }

  /**
   * @desc 更新
   * @param model
   * @param values
   * @param [options]
   * @param [options.transaction] 事务
   * @param {Array|Object} [cacheRemove]
   * @param cacheRemove.Model
   * @param cacheRemove.Parm
   * @return {Promise<*>}
   */
  async Update (model, values, options, cacheRemove) {
    this.CacheRemoveList(cacheRemove)
    const retValue = await KOCReturn.Promise(() => model.update(values, options))
    if (retValue.hasError) return retValue
    retValue.returnObject = retValue.returnObject[0]
    return retValue
  }

  Update_Raw (model, sql, options, cacheRemove) {

  }

  /**
   * @desc 查询一条记录
   * @param model
   * @param [options]
   * @param [options.transaction] 事务
   * @param [options.raw] 是否返回纯数据 默认 否
   * @param [cache]
   * @param cache.Model
   * @param cache.Parm
   * @return {Promise<*>}
   */
  async Info (model, options = {}, cache) {
    let retValue
    // 读取缓存数据
    if (cache && !options.transaction) {
      retValue = await this.CacheGet(cache.Model, cache.Parm)
      if (!retValue.hasError && retValue.returnObject) {
        if (options && options.raw !== true) retValue.returnObject = model.build(retValue.returnObject)
        return retValue
      }
    }
    retValue = await KOCReturn.Promise(() => model.findOne(options))
    // 写入缓存数据
    if (!retValue.hasError && retValue.returnObject) await this.CachePut(cache.Model, cache.Parm, retValue.returnObject)
    return retValue
  }

  /**
   * @desc 缓存key
   * @param model
   * @param parm
   * @return {string}
   */
  CacheKey (model, parm) {
    return KOCString.MD5(JSON.stringify(model) + JSON.stringify(parm))
  }

  /**
   * @desc 缓存过期时间
   * @param expire 过期时间 分
   * @return {number}
   */
  CacheExpire (expire = 3) {
    return KOCString.ToIntPositive(expire, 3) * 60
  }

  /**
   * @desc 设置缓存
   * @param model
   * @param parm
   * @param data
   * @param expire
   */
  CachePut (model, parm, data, expire) {
    if (!this.redisClient || !data) return
    this.redisClient.set(this.CacheKey(model, parm), JSON.stringify(data), 'EX', this.CacheExpire(expire))
  }

  /**
   * @desc 获取缓存
   * @param model
   * @param parm
   * @return {Promise<*>|KOCReturn}
   */
  async CacheGet (model, parm) {
    if (!this.redisClient) return new KOCReturn()
    let retValue = await KOCReturn.Promise(() => this.redisClient.get(this.CacheKey(model, parm)))
    if (!retValue.hasError && retValue.returnObject) {
      try {
        retValue.returnObject = JSON.parse(retValue.returnObject)
        // 写入缓存数据
        await this.CachePut(model, parm, retValue.returnObject)
        return retValue
      } catch {
        retValue = new KOCReturn({ hasError: true })
      }
    }
    return retValue
  }

  /**
   * @desc 删除缓存
   * @param model
   * @param parm
   * @return {Promise<*>|KOCReturn}
   */
  CacheRemove (model, parm) {
    if (!this.redisClient) return new KOCReturn()
    return KOCReturn.Promise(() => this.redisClient.del(this.CacheKey(model, parm)))
  }

  /**
   * @desc 批量删除缓存
   * @param {Array|Object} list
   * @param list.Model
   * @param list.Parm
   * @return {KOCReturn}
   */
  CacheRemoveList (list) {
    if (!this.redisClient) return new KOCReturn()
    list = KOCString.ToArray(list)
    if (list.length > 0) return new KOCReturn()
    let retValue = new KOCReturn()
    for (const thisValue of list) {
      retValue = this.CacheRemove(thisValue.Model, thisValue.Parm)
    }
    return retValue
  }

}

module.exports = {
  KOCMysql,
  DataTypes,
  Op
}
