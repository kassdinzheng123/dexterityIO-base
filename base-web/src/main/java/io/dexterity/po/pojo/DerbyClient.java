package io.dexterity.po.pojo;

import io.dexterity.config.MyConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.*;
@Component
public class DerbyClient {
    @Autowired
    MyConfig myConfig;
    private static final String DB_URL = "jdbc:derby:"+MyConfig.path+"Derby;create=true";
    private Connection conn;
    private Statement stmt;
    public DerbyClient() throws SQLException {
        conn = DriverManager.getConnection(DB_URL);
        stmt = conn.createStatement();
    }

    /**
     * 查询总条数
     */
    public int selectCount(String tableName) throws SQLException {
        int count = 0;
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
        if (rs.next()) {
            count = rs.getInt(1);
        }
        return count;
    }

    /**
     * 关闭客户端
     */
    public void close() throws SQLException {
        stmt.close();
        conn.close();
    }

    /**
     * 创建表
     */
    public void createTable(String tableName, String... columns) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append("(");
        for (int i = 0; i < columns.length; i++) {
            sb.append(columns[i]);
            if (i < columns.length - 1) {
                sb.append(",");
            }
        }
        sb.append(")");
        stmt.executeUpdate(sb.toString());
    }

    /**
     * 查询库中的所有表信息
     * @throws SQLException
     */
    public void listTable() throws SQLException {
        String sql = "SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE='T'";
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            String tableName = rs.getString("TABLENAME");
            System.out.println(tableName);
        }
    }

    /**
     * 删除表
     */
    public void deleteTable(String tableName) throws SQLException{
        String sql = "DROP TABLE " + tableName;
        stmt.executeUpdate(sql);
    }
}
