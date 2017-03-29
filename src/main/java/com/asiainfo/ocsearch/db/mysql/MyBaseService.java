package com.asiainfo.ocsearch.db.mysql;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;


public class MyBaseService {

	private final QueryRunner queryRunner;

	private static  MyBaseService instance=null;

	public static synchronized  MyBaseService getInstance(){
		if(instance==null)
			instance=new MyBaseService("config");
		return instance;
	}

	private MyBaseService() {
		this.queryRunner = new QueryRunner(DataSourceProvider.getDataSource());
	}

	private MyBaseService(String dataSource) {
		BasicDataSource bds = DataSourceProvider.getDataSource(dataSource);
		this.queryRunner = new QueryRunner(bds);
	}

	public Map<String, Object> queryOne(String sql) throws SQLException {
		return (Map) this.queryRunner.query(sql, new MapHandler());
	}

	public List<Map<String, Object>> queryList(String sql) throws SQLException {
		return (List) this.queryRunner.query(sql, new MapListHandler());
	}

	public List<Map<String, Object>> queryList(String sql, Object... params)
			throws SQLException {
		return (List) this.queryRunner.query(sql, new MapListHandler(), params);
	}

	public int insertOrUpdate(String sql, Object... params) throws SQLException {
		return this.queryRunner.update(sql, params);
	}

	public int delete(String sql) throws SQLException {
		return this.queryRunner.update(sql);
	}

	public int update(String sql) throws SQLException {
		return this.queryRunner.update(sql);
	}
	
	public int[] batch(String sql, Object[][] params) throws SQLException {
		return this.queryRunner.batch(sql, params);
	}

}
