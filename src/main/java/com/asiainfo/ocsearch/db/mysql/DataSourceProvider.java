package com.asiainfo.ocsearch.db.mysql;

import com.asiainfo.ocsearch.utils.PropertiesLoadUtil;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
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
	
	public static void batchSave(String tableName,List<Map<String, Object>> fieldMaps){
		Connection conn=null;
		Statement stmt=null;
		try {
			conn=getConnection();
			conn.setAutoCommit(false);
			stmt=conn.createStatement();
			
			for(Map<String, Object> fieldMap:fieldMaps){
				stmt.addBatch(prepareSQL(tableName, fieldMap));
			}

			 stmt.executeBatch();    //执行批处理 
             conn.commit();
             stmt.close();
             conn.close();
             
             
			
		} catch (SQLException e) {

			logger.error(e);
			try {
				conn.rollback();
			} catch (SQLException e1) {

				logger.error(e1);
			}
		}
		
		finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
			
			if(stmt!=null){
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
		}
		
	}
	
	public static void save(String tableName,Map<String, Object> fieldMap){
		Connection conn=null;
		Statement stmt=null;
		try {
			
			String sql=prepareSQL(tableName, fieldMap);
			conn=getConnection();
			conn.setAutoCommit(false);
			stmt=conn.createStatement();
			stmt.execute(sql);
			conn.commit();
			stmt.close();
			conn.close();
			
		} catch (SQLException e) {
			
			try {
				conn.rollback();
			} catch (SQLException e1) {

				logger.error(e1);
			}
			
		}
		
		finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
			
			if(stmt!=null){
				try {
					stmt.close();
				} catch (SQLException e) {
					logger.error(e);
				}
			}
		}
		
	}
	
    public static String prepareSQL(String tableName,Map<String, Object> fieldMap){
		
		StringBuffer sb=new StringBuffer("insert into "+tableName+"(");
		Iterator<String> it=fieldMap.keySet().iterator();
		StringBuffer keys=new StringBuffer();
		StringBuffer values=new StringBuffer();
		while(it.hasNext()){
			String key=it.next();
			keys.append(StringUtils.lowerCase(key)+",");
			Object value=fieldMap.get(key);
			
			if(value instanceof String){
				value="'"+value.toString()+"'";
			}
			values.append(value+",");
		}
		keys=new StringBuffer(keys.substring(0,keys.length()-1));
		values=new StringBuffer(values.substring(0, values.length()-1));
		sb.append(keys).append(") values (").append(values).append(")");
        		
		return sb.toString();
		
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
	public static void main(String[] args){
		try{
		   Connection conn = DataSourceProvider.getConnection("config");
		   conn.setAutoCommit(false);
		   
		   
		   
		   String sql = "insert into tablename(name,age)values(?,?);";
		   PreparedStatement pstmt = conn.prepareStatement(sql);
		   pstmt.setString(1, "AOBAMA");
		   pstmt.setInt(2, 45);
		   pstmt.executeUpdate();
		   pstmt.close();
		   conn.commit();
		   
		   Statement st = conn.createStatement();
		   st.setQueryTimeout(1000);
		   st.setFetchSize(10);
		   ResultSet rs = st.executeQuery("select * from tablename");
		   while(rs.next()){
			   System.out.print("name="+rs.getString(1));
			   System.out.print("name="+rs.getInt(2));
		   }
		   st.close();
		   conn.close();
		  }catch(Exception e){
			  logger.error(e);
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
