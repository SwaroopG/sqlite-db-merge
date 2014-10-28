package com.poorjar.sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;

/**
 * @author Swaroop
 */
public final class MergeUtility {

    private static String query = "SELECT va.asset_id, vr.url FROM vod_resource as vr JOIN vod_asset as va ON vr.asset_id=va.asset_id ORDER BY va.asset_id";

    /**
     * a
     * @throws Exception a
     */
    public static void mergeDB() throws Exception {
        String centralDB = "jdbc:sqlite:" + MergeUtility.class.getResource("/central.db").getPath();
        String regionalDB = "jdbc:sqlite:" + MergeUtility.class.getResource("/regional.db").getPath();

        // Central DB
        Connection connx1 = null;
        Connection connx2 = null;

        try {
            connx1 = getConnection(centralDB);
            connx2 = getConnection(regionalDB);

            Statement stmt1 = connx1.createStatement();
            ResultSet rs1 = stmt1.executeQuery(query);

            // Connect Regional DB
            Statement stmt2 = connx2.createStatement();
            ResultSet rs2 = stmt2.executeQuery(query);

            merge(rs1, rs2);

            DbUtils.close(stmt1);
            DbUtils.close(rs1);
            DbUtils.close(stmt2);
            DbUtils.close(rs2);
        } catch (Exception e) {
            // throw DigeoException
            throw(e);
        } finally {
            // CloseQuietly will take care of the connx=null case as well.
            DbUtils.closeQuietly(connx1);
            DbUtils.closeQuietly(connx2);
        }
    }

    /**
     * Establishes a connection to the database.
     * @param connectionString a
     * @return Connection
     * @throws Exception a
     */
    public static Connection getConnection(String connectionString) throws Exception {
        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection(connectionString);
    }

    private static void merge(ResultSet central, ResultSet regional) throws SQLException {
        while (central.next()) {
            System.out.println(central.getString(1) + " | " + central.getString(2));
        }
        
        while(regional.next()) {
            System.out.println(regional.getString(1) + " | " + regional.getString(2));
        }
    }
}
