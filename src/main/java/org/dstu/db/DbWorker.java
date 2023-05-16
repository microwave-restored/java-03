package org.dstu.db;

import org.dstu.util.CsvReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DbWorker {
    public static void populateFromFile(String fileName) {
        List<String[]> strings = CsvReader.readCsvFile(fileName, ";");
        Connection conn = DbConnection.getConnection();
        try {
            Statement cleaner = conn.createStatement();
            System.out.println(cleaner.executeUpdate("DELETE FROM tv"));
            System.out.println(cleaner.executeUpdate("DELETE FROM monitor"));
            PreparedStatement tvSt = conn.prepareStatement(
                    "INSERT INTO tv (manufacturer, model, diagonal, resolution, matrix, wifi, smarttv) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)");
            PreparedStatement monitorSt = conn.prepareStatement(
                    "INSERT INTO monitor (manufacturer, model, diagonal, resolution, matrix, freshrate, synctype) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)");

            for (String[] line: strings) {
                if (line[0].equals("0")) {
                    tvSt.setString(1, line[1]);
                    tvSt.setString(2, line[2]);
                    tvSt.setString(3, line[3]);
                    tvSt.setString(4, line[4]);
                    tvSt.setString(5, line[5]);
                    tvSt.setBoolean(6, Boolean.parseBoolean(line[6]));
                    tvSt.setString(7, line[7]);
                    tvSt.addBatch();
                } else {
                    monitorSt.setString(1, line[1]);
                    monitorSt.setString(2, line[2]);
                    monitorSt.setString(3, line[3]);
                    monitorSt.setString(4, line[4]);
                    monitorSt.setString(5, line[5]);
                    monitorSt.setInt(6, Integer.parseInt(line[6]));
                    monitorSt.setString(7, line[7]);
                    monitorSt.addBatch();
                }
            }
            int[] stRes = tvSt.executeBatch();
            int[] monitorRes = monitorSt.executeBatch();
            for (int num: stRes) {
                System.out.println(num);
            }

            for (int num: monitorRes) {
                System.out.println(num);
            }
            cleaner.close();
            tvSt.close();
            monitorSt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void demoQuery() {
        Connection conn = DbConnection.getConnection();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM monitor WHERE freshrate > 100");
            while (rs.next()) {
                System.out.print(rs.getString("manufacturer"));
                System.out.print(" ");
                System.out.print(rs.getString("model"));
                System.out.print(" ");
                System.out.println(rs.getString("freshrate"));
            }
            rs.close();
            st.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void dirtyReadDemo() {
        Runnable first = () -> {
            Connection conn1 = DbConnection.getNewConnection();
            if (conn1 != null) {
                try {
                    conn1.setAutoCommit(false);
                    conn1.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement upd = conn1.createStatement();
                    upd.executeUpdate("UPDATE tv SET resolution='2160p' WHERE resolution='4K'");
                    Thread.sleep(2000);
                    conn1.rollback();
                    upd.close();
                    Statement st = conn1.createStatement();
                    System.out.println("In the first thread:");
                    ResultSet rs = st.executeQuery("SELECT * FROM tv");
                    while (rs.next()) {
                        System.out.println(rs.getString("resolution"));
                    }
                    st.close();
                    rs.close();
                    conn1.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };

        Runnable second = () -> {
            Connection conn2 = DbConnection.getNewConnection();
            if (conn2 != null) {
                try {
                    Thread.sleep(500);
                    conn2.setAutoCommit(false);
                    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                    Statement st = conn2.createStatement();
                    ResultSet rs = st.executeQuery("SELECT * FROM tv");
                    while (rs.next()) {
                        System.out.println(rs.getString("resolution"));
                    }
                    rs.close();
                    st.close();
                    conn2.close();
                } catch (SQLException | InterruptedException throwables) {
                    throwables.printStackTrace();
                }
            }
        };
        Thread th1 = new Thread(first);
        Thread th2 = new Thread(second);
        th1.start();
        th2.start();
    }
}
