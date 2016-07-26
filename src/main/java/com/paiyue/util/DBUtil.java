package com.paiyue.util;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
	
public class DBUtil {
	private static Connection conn = null;

	public static Connection getConnection() {
		try {
			
			String driver = "com.mysql.jdbc.Driver";
			
			String url = "jdbc:mysql://10.1.0.39:3306/pureget?useUnicode=true&characterEncoding=GBK";
			
			// MySQL配置时的用户名
			String user = "report";
			// Java连接MySQL配置时的密码
			String password = "paireport";
			// 加载驱动程序
			Class.forName(driver);
			// 连续数据库
			conn = DriverManager.getConnection(url, user, password);
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			System.out.println("SQLException: " + e.getMessage());
			System.out.println("SQLState: " + e.getSQLState());
			System.out.println("VendorError: " + e.getErrorCode());
		} finally {
			
		}
		return conn;
	}
	
	public static void closeAll(Connection conn,PreparedStatement ps,ResultSet rs){
		if(rs!=null){
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(ps!=null){
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}