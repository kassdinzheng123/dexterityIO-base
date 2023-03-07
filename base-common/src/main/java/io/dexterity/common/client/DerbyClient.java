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
     * 创建一个新表
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
     * 向表中插入数据
     */
    public void insert(String tableName, Object... values) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName).append(" VALUES(");
        for (int i = 0; i < values.length; i++) {
            sb.append("?");
            if (i < values.length - 1) {
                sb.append(",");
            }
        }
        sb.append(")");
        PreparedStatement ps = conn.prepareStatement(sb.toString());
        for (int i = 0; i < values.length; i++) {
            ps.setObject(i + 1, values[i]);
        }
        ps.executeUpdate();
    }


    public static void main(String[] args) throws SQLException {
        DerbyClient client = new DerbyClient();
        client.createTable(
                "bucket",
                "bucket_id INT PRIMARY KEY",
                "bucket_name VARCHAR(60)",
                "access_authority VARCHAR(60)",
                "domain_name VARCHAR(60)",
                "region VARCHAR(60)");
//        client.insert("users", 1, "Alice", 20);
//        client.insert("users", 2, "Bob", 25);
//        ResultSet rs = client.select("bucket");
//        while (rs.next()) {
//            int id = rs.getInt("id");
//            String name = rs.getString("name");
//            int age = rs.getInt("age");
//            System.out.println(id + ", " + name + ", " + age);
//        }

        client.close();
    }
}
