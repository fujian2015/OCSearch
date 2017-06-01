package com.asiainfo.ocsearch.datasource.jdbc.phoenix;


import com.asiainfo.ocsearch.datasource.jdbc.HbaseJdbcHelper;
import com.asiainfo.ocsearch.datasource.jdbc.pool.DbPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * phoenix的jdbc支持工具类(连接都是从com.ailk.oci.ocnosql.client.jdbc.pool.DbPool获取的)<br>
 * 注意：<br>
 * <ul>
 * <li>多个操作请使用同一个PhoenixJdbcHelper实例，以保证多个操作在同一个事务之下</li>
 * <li>PhoenixJdbcHelper用完之后一定要记得使用close(底层实际上是关闭数据库连接)</li>
 * <li>Phoenix目前对PreparedStatement支持的不是很好，使用的时候会报
 * java.sql.SQLFeatureNotSupportedException，但是不会影响结果</li>
 * </ul>
 * 
 *
 * 
 */
public class PhoenixJdbcHelper implements HbaseJdbcHelper {
	static Log LOG = LogFactory.getLog(PhoenixJdbcHelper.class);

	public PhoenixJdbcHelper() {
		// 初始化的时候获取一个连接放在当前线程里
		DbPool.getConnection();
	}

	@Override
	public int excuteNonQuery(String sql) throws SQLException {
		Connection connection = DbPool.getConnection();
		Statement statement = connection.createStatement();
		return statement.executeUpdate(sql);
	}

	@Override
	public void excuteNonQuery(String[] sqls, int batchSize)
			throws SQLException {
		/**
		 * Phoenix的statement不支持addBatch
		 */
		Connection connection = DbPool.getConnection();
		connection.setAutoCommit(false);
		int rowCount = 0;
		for (String sql : sqls) {
			Statement statement = connection.createStatement();
			statement.executeUpdate(sql);
			if (++rowCount % batchSize == 0) {
				connection.commit();
				System.out.println("Rows upserted: " + rowCount);
			}
		}
		connection.commit();
	}

	@Override
	public int excuteNonQuery(String sql, Object[] args) throws SQLException {
		Connection connection = DbPool.getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		if (null != args) {
			for (int i = 0; i < args.length; i++) {
				preparedStatement.setObject(i + 1, args[i]);
			}
		}
		return preparedStatement.executeUpdate();
	}

	@Override
	public void excuteNonQuery(String sqls[], List<Object[]> args, int batchSize)
			throws SQLException {
		Connection connection = DbPool.getConnection();
		connection.setAutoCommit(false);
		int rowCount = 0;
		for (String sql : sqls) {

			PreparedStatement preparedStatement = connection
					.prepareStatement(sql);
			Object[] ars = args.get(rowCount);
			if (null != ars) {
				for (int i = 0; i < ars.length; i++) {
					preparedStatement.setObject(i + 1, ars[i]);
				}
			}
			preparedStatement.executeUpdate();

			if (++rowCount % batchSize == 0) {
				connection.commit();
				System.out.println("Rows upserted: " + rowCount);
			}
		}
		connection.commit();
	}

	@Override
	public ResultSet executeQueryRaw(String sql) throws SQLException {
		ResultSet rsResultSet = null;
		Connection connection = DbPool.getConnection();
		Statement statement = connection.createStatement();
		rsResultSet = statement.executeQuery(sql);
		return rsResultSet;
	}

	@Override
	public ResultSet executeQueryRaw(String sql, Object[] args)
			throws SQLException {
		ResultSet rsResultSet = null;
		Connection connection = DbPool.getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		if (null != args) {
			for (int i = 0; i < args.length; i++) {
				preparedStatement.setObject(i + 1, args[i]);
			}
		}
		rsResultSet = preparedStatement.executeQuery();
		return rsResultSet;
	}

	@Override
	public List<Map<String, Object>> executeQuery(String sql)
			throws SQLException {
		ResultSet rsResultSet = null;
		Connection connection = DbPool.getConnection();
		Statement statement = connection.createStatement();
		rsResultSet = statement.executeQuery(sql);

		int columnCount = rsResultSet.getMetaData().getColumnCount();
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		while (rsResultSet.next()) {
			Map<String, Object> map = new HashMap<String, Object>();
			for (int i = 1; i <= columnCount; i++) {
				map.put(rsResultSet.getMetaData().getColumnName(i),
						rsResultSet.getObject(i));
			}
			list.add(map);
		}
		return list;
	}

	@Override
	public List<Map<String, Object>> executeQuery(String sql, Object[] args)
			throws SQLException {
		ResultSet rsResultSet = null;
		Connection connection = DbPool.getConnection();
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		if (null != args) {
			for (int i = 0; i < args.length; i++) {
				preparedStatement.setObject(i + 1, args[i]);
			}
		}
		rsResultSet = preparedStatement.executeQuery();

		int columnCount = rsResultSet.getMetaData().getColumnCount();
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		while (rsResultSet.next()) {
			Map<String, Object> map = new HashMap<String, Object>();
			for (int i = 1; i <= columnCount; i++) {
				map.put(rsResultSet.getMetaData().getColumnName(i),
						rsResultSet.getObject(i));
			}
			list.add(map);
		}
		return list;
	}

	@Override
	public void beginTransaction() throws SQLException {
		DbPool.beginTransaction();
	}

	@Override
	public void commitTransaction() throws SQLException {
		DbPool.commitTransaction();
	}

	@Override
	public void rollbackTransaction() throws SQLException {
		DbPool.rollbackTransaction();
	}

	@Override
	public void close() throws SQLException {
		DbPool.closeConnection();
	}

}
