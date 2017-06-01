package com.asiainfo.ocsearch.datasource.jdbc.pool;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * 单例的连接池工具类 使用连接池的时候并不是在代码中不用获取/释放数据库连接，而是在代码中向连接池申请/释放连接，对于代码而言，可以把连接池看成数据库。
 * 换句话说 ，连接池就是数据库的代理，之所以要使用这个代理是因为直接向数据库申请/释放连接是要降低性能的：如果每一次数据访问请求都必须经历建立数据库连接、
 * 打开数据库 、存取数据和关闭数据库连接等步骤，而连接并打开数据库是一件既消耗资源又费时的工作，那么频繁发生这种数据库操作时，系统的性能必然会急剧下降。
 * 连接池的作用是自己维护数据库连接，数据库连接池的主要操作如下： 　　（1）建立数据库连接池对象（服务器启动）。
 * 　　（2）按照事先指定的参数创建初始数量的数据库连接（即：空闲连接数）。
 * 　　（3）对于一个数据库访问请求，直接从连接池中得到一个连接。如果数据库连接池对象中没有空闲的连接
 * ，且连接数没有达到最大（即：最大活跃连接数），创建一个新的数据库连接。 　　（4）存取数据库。
 * 　　（5）关闭数据库，释放所有数据库连接（此时的关闭数据库连接，并非真正关闭，而是将其放入空闲队列中。如实际空闲连接数大于初始空闲连接数则释放连接）。
 * 　　（6）释放数据库连接池对象（服务器停止、维护期间，释放数据库连接池对象，并释放所有连接）
 *
 *
 */
public final class DbPool {
    private final static Logger log = Logger.getLogger(DbPool.class);
    // 创建一个私有静态的并且是与事务相关联的局部线程变量
    public static ThreadLocal<Connection> connectionHolder = new ThreadLocal<Connection>();
    private static DruidDataSource dataSource;

    private DbPool() {
    }

    public static void setUp(Properties druid, Properties hbase) {
        synchronized (DbPool.class) {
            if (null == dataSource) {
                dataSource = new DruidDataSource();
                String jdbcUrl = hbase.getProperty("jdbcUrl", "jdbc:phoenix");
                String quorum = hbase.getProperty("hbase.zookeeper.quorum", null);
                String clientPort = hbase.getProperty("hbase.zookeeper.property.clientPort", null);

                if (null == quorum || null == clientPort) {
                    throw new RuntimeException("[quorum or clientPort] not config in client-runtime.properties");
                }
                jdbcUrl = jdbcUrl + ":" + quorum + ":" + clientPort;
                log.info("jdbcUrl=" + jdbcUrl);

                druid.setProperty("url", jdbcUrl);
                try {
                    druid.list(System.out);

                    for (Map.Entry entry : druid.entrySet()) {
                        BeanUtils.setProperty(dataSource, (String) entry.getKey(), entry.getValue());
                    }
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 获取连接<br>
     * 注意：获取的连接默认是自动提交事务的，如果要自己控制事物，请先使用beginTransaction开启事物，使用commitTransaction提交事务
     *
     * @return
     * @throws SQLException
     */
    public static final Connection getConnection() {

        // 从线程变量connectionHolder中获取连接
        Connection conn = connectionHolder.get();
        // 如果在当前线程中没有绑定相应的Connection
        if (conn == null) {
            try {
                conn = dataSource.getConnection();
                // 将Connection设置到ThreadLocal线程变量中
                connectionHolder.set(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    /**
     * 关闭连接和从线程变量中删除连接
     */
    public static void closeConnection() {
        Connection conn = connectionHolder.get();
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                // 从ThreadLocal中清除Connection
                connectionHolder.remove();
            }
        }
    }

    /**
     * 开启事务，手动开启
     */
    public static void beginTransaction() {
        Connection conn = connectionHolder.get();
        try {
            // 如果连接存在，再设置连接，否则会出错
            if (conn != null) {
                // 默认conn是自动提交，
                if (conn.getAutoCommit()) {
                    // 关闭自动提交，即是手动开启事务
                    conn.setAutoCommit(false);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 提交事物
     */
    public static void commitTransaction() {
        Connection conn = connectionHolder.get();
        if (conn != null) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 回滚事务
     */
    public static void rollbackTransaction() {
        Connection conn = connectionHolder.get();
        try {
            if (conn != null) {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}