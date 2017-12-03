package cn.tao.logenhance;


import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import com.mysql.jdbc.Connection;


public class DBLoader {
	
	public static void dbLoader(HashMap<String,String> ruleMap){
		Connection conn=null;
		Statement stmt=null;
		ResultSet rs=null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn=(Connection) DriverManager.getConnection("jdbc:mysql://10.108.21.2:3306/tao","root","123456");
			stmt=conn.createStatement();
			rs=stmt.executeQuery("select url,content from url_rule");
			while(rs.next()){
				ruleMap.put(rs.getString(1), rs.getString(2));
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
				try {
					if(rs!=null){
					    rs.close();
					}
					if(stmt!=null){
						stmt.close();
					}
					if(conn!=null){
						conn.close();
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
		}
	}
	
	public static void main(String[] args) {
		DBLoader db=new DBLoader();
		HashMap<String, String> map=new HashMap<String,String>();
		db.dbLoader(map);
		System.out.println(map.size());
	}
}
