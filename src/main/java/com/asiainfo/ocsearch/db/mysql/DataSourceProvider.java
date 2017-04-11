package com.asiainfo.ocsearch.db.mysql;

import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;


public class DataSourceProvider {
	private static Map dsMap = new HashMap();
	private static Logger logger=Logger.getLogger(DataSourceProvider.class);

	static {
		try {
			loadProps("conf-db.properties");
		} catch (Exception e) {
			logger.error("database.properties配置不正确!!!", e);
		}
	}


	private static void loadProps(String dbPropName) {
		Properties properties = PropertiesLoadUtil.loadProFile(dbPropName);
		for (Iterator iterator = properties.keySet().iterator(); iterator
				.hasNext();) {
			String key = (String) iterator.next();
			String db = key.split("\\.")[0];
			if (!dsMap.containsKey(db)) {
				BasicDataSource ds = new BasicDataSource();
				ds.setDriverClassName(properties.getProperty(db + ".driver"));
				ds.setUrl(properties.getProperty(db + ".url"));
				ds.setUsername(properties.getProperty(db + ".username"));
				ds.setPassword(properties.getProperty(db + ".password"));
				ds.setInitialSize(10);
				ds.setMaxActive(50);

				ds.setTimeBetweenEvictionRunsMillis(3600000L);
				ds.setMinEvictableIdleTimeMillis(3600000L);
				ds.setTestWhileIdle(true);
				ds.setTestOnBorrow(true);
				ds.setValidationQuery("select 1 from dual");
				dsMap.put(db, ds);
			}
		}
	}

	public static synchronized Connection getConnection(String name) {
		BasicDataSource ds = (BasicDataSource) dsMap.get(name);
		Connection connection = null;
		try {
			connection = ds.getConnection();
		} catch (SQLException e) {
			logger.error(e);
		}
		return connection;
	}

	public static Connection getConnection() {
		return getConnection("default");
	}

	public static BasicDataSource getDataSource() {
		return (BasicDataSource) dsMap.get("default");
	}

	public static BasicDataSource getDataSource(String dataSource) {
		return (BasicDataSource) dsMap.get(dataSource);
	}

	public static Set dbNameSet() {
		return dsMap.keySet();
	}

	public static void printDatabaseStatus() {
		for (Iterator iterator = dsMap.keySet().iterator(); iterator.hasNext();) {
			String dsName = (String) iterator.next();
			BasicDataSource bds = (BasicDataSource) dsMap.get(dsName);
			String head = "========== " + dsName
					+ "\tconnection status ==========";
			logger.info(head);
			logger.info("Active\tconnection number: " + bds.getNumActive());
			logger.info("Idle  \tconnection number: " + bds.getNumIdle());
		}
	}
}
