package com.asiainfo.ocsearch.datasource.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;


public interface HbaseJdbcHelper {
	/**
	 * 以statement的方式执行单条sql语句，一般是以下两种sql:<br>
	 * 1--DML<br>
	 * 0--DDL<br>
	 * DQL请使用executeQueryRaw方法<br>
	 * 注意：执行单条语句会自动提交，连续执行本方法请开启事物
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public int excuteNonQuery(String sql) throws SQLException;

	/**
	 * 以PreparedStatement的方式执行单条sql语句，一般是以下两种sql:<br>
	 * 1--DML<br>
	 * 0--DDL<br>
	 * DQL请使用executeQueryRaw方法<br>
	 * 注意：执行单条语句会自动提交，连续执行本方法请开启事物
	 * 
	 * @param sql
	 * @param args
	 *            PreparedStatement参数，如果确实没有参数可以为null
	 * @return
	 * @throws SQLException
	 */
	public int excuteNonQuery(String sql, Object[] args) throws SQLException;

	/**
	 * 以statement方式批量执行多条sql(按照指定的批量数提交，无需自己控制事务)
	 * 
	 * @param sqls
	 * @param batchSize
	 *            批量
	 * @throws SQLException
	 *             注意：大批量的导入操作，请使用
	 */
	public void excuteNonQuery(String[] sqls, int batchSize)
			throws SQLException;

	/**
	 * 以PreparedStatement方式批量执行多条sql(按照指定的批量数提交，无需自己控制事务)
	 * 
	 * @param sqls
	 * @param args
	 *            PreparedStatement参数，如果确实没有参数可以为null
	 * @param batchSize
	 *            批量
	 * @throws SQLException
	 */
	public void excuteNonQuery(String[] sqls, List<Object[]> args, int batchSize)
			throws SQLException;

	/**
	 * 以statement的方式执行单条查询语句并返回原始的结果集<br>
	 * 注意：查询语句无需考虑事务问题
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public ResultSet executeQueryRaw(String sql) throws SQLException;

	/**
	 * 以PreparedStatement的方式执行单条查询语句并返回原始的结果集<br>
	 * 注意：查询语句无需考虑事务问题
	 * 
	 * @param sql
	 * @param args
	 *            PreparedStatement参数，如果确实没有参数可以为null
	 * @return
	 * @throws SQLException
	 */
	public ResultSet executeQueryRaw(String sql, Object[] args)
			throws SQLException;

	/**
	 * 以statement的方式执行单条查询语句并返回包装后的结果集<br>
	 * 注意：查询语句无需考虑事务问题
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public List<Map<String, Object>> executeQuery(String sql)
			throws SQLException;

	/**
	 * 以PreparedStatement的方式执行单条查询语句并返回包装后的结果集<br>
	 * 注意：查询语句无需考虑事务问题
	 * 
	 * @param sql
	 * @param args
	 * @return
	 * @throws SQLException
	 */
	public List<Map<String, Object>> executeQuery(String sql, Object[] args)
			throws SQLException;

	/**
	 * 开启事务<br>
	 * 注意：<br>
	 * <ul>
	 * <li>单条语句不需要开事务(会自动提交的)</li>
	 * <li>一旦开启了事务，请记得提交事务，否则语句是不会执行的</li>
	 * </ul>
	 * 
	 * @throws SQLException
	 */
	public void beginTransaction() throws SQLException;

	/**
	 * 提交事务
	 * 
	 * @throws SQLException
	 */
	public void commitTransaction() throws SQLException;

	/**
	 * 回滚事务
	 * 
	 * @throws SQLException
	 */
	public void rollbackTransaction() throws SQLException;

	/**
	 * 关闭连接
	 * 
	 * @throws SQLException
	 */
	public void close() throws SQLException;
}
