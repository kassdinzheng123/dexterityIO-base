package io.dexterity.common.client;

import java.sql.*;

public class DerbyClient {
    private static final String DB_URL = "jdbc:derby:dexterityData;create=true";
    private Connection conn;
    private Statement stmt;
    public DerbyClient() throws SQLException {
        conn = DriverManager.getConnection(DB_URL);
        stmt = conn.createStatement();
    }

    /**
     * 查询该表所有的信息
     */
    public ResultSet select(String tableName) throws SQLException {
        return stmt.executeQuery("SELECT * FROM " + tableName);
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

    public static void main(String[] args) throws SQLException {
        DerbyClient client = new DerbyClient();
        client.deleteTable("BUCKET");
        client.createTable(
                "bucket",
                "bucket_id INT PRIMARY KEY",
                "bucket_name VARCHAR(255)",
                "access_authority VARCHAR(255)",
                "domain_name VARCHAR(255)",
                "region VARCHAR(255)",
                "status INT",
                "tags VARCHAR(255)");
        client.listTable();
        client.close();
    }
}
