package dao;
import org.openstreetmap.osm._0.Node;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;


public class NodeDAO implements DAO<Node> {
    private Connection connection;
    private long timePassed;
    private int records;

    public NodeDAO(Connection conn) {
        this.connection = conn;
        timePassed = 0;
        records = 0;
    }

    @Override
    public void insertBatch(ArrayList<Node> list, PreparedStatement statement) {
        Timestamp timestamp;
        try {
            for (Node node : list) {
                timestamp = new Timestamp(node.getTimestamp().toGregorianCalendar().getTimeInMillis());
                statement.setBigDecimal(1, new BigDecimal(node.getId()));
                statement.setBigDecimal(2, new BigDecimal(node.getVersion()));
                statement.setTimestamp(3, timestamp);
                statement.setBigDecimal(4, new BigDecimal(node.getUid()));
                statement.setString(5, node.getUser());
                statement.setBigDecimal(6, new BigDecimal(node.getChangeset()));
                statement.setDouble(7, node.getLat());
                statement.setDouble(8, node.getLon());
                statement.addBatch();
            }
            long startTime = System.currentTimeMillis();
            statement.executeBatch();
            connection.commit();
            long finishTime = System.currentTimeMillis();
            this.timePassed += finishTime - startTime;
            this.records += list.size();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void insertPreparedStatement(Node node, PreparedStatement statement) {
        Timestamp timestamp = new Timestamp(node.getTimestamp().toGregorianCalendar().getTimeInMillis());
        try {
            statement.setBigDecimal(1, new BigDecimal(node.getId()));
            statement.setBigDecimal(2, new BigDecimal(node.getVersion()));
            statement.setTimestamp(3, timestamp);
            statement.setBigDecimal(4, new BigDecimal(node.getUid()));
            statement.setString(5, node.getUser());
            statement.setBigDecimal(6, new BigDecimal(node.getChangeset()));
            statement.setDouble(7, node.getLat());
            statement.setDouble(8, node.getLon());
            long startTime = System.currentTimeMillis();
            statement.executeUpdate();
            connection.commit();
            long finishTime = System.currentTimeMillis();
            this.timePassed += finishTime - startTime;
            this.records += 1;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void insertStatement(Node node, Statement statement) {
        String sql;
        try {
            connection.setAutoCommit(false);
            Timestamp timestamp = new Timestamp(node.getTimestamp().toGregorianCalendar().getTimeInMillis());
            sql = "INSERT INTO NODES (ID, VERSION, _TIMESTAMP, UID, USER_NAME, CHANGESET, LAT, LON) VALUES " +
                    "(" +
                    node.getId() + ", " +
                    node.getVersion() + ", " + "'" +
                    timestamp + "'" + ", " +
                    node.getUid() + ", " + "'" +
                    node.getUser().replace('\'', '.') + "'" + ", " +
                    node.getChangeset() + ", " +
                    node.getLat().toString().replace(',', '.') + ", " +
                    node.getLon().toString().replace(',', '.') +
                    ");";
            long startTime = System.currentTimeMillis();
            statement.executeUpdate(sql);
            connection.commit();
            long finishTime = System.currentTimeMillis();
            this.timePassed += finishTime - startTime;
            this.records += 1;
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }
    }

    public long timeSpent() {
        if (timePassed == 0) {
            return 0;
        }
        else {
            return records * 1000 / timePassed;
        }
    }
}

