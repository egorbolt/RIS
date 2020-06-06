package database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgresDB {
    private static Logger log = LoggerFactory.getLogger(PostgresDB.class);
    public static final String username = "root";
    public static final String url = "jdbc:postgresql://localhost:5432/test123" ;
    public static final String password = "12345678";
    private Connection connection;

    public PostgresDB() {
        connectToDB();
    }

    public void connectToDB() {
        try {
            if (connection != null) {
                connection = DriverManager.getConnection(url, username, password);
            }
            log.info("Connection established");
        } catch (Exception e) {
            log.error("Error: something wrong happened while connecting to DB");
        }
    }

    public void disconnectFromDB() {
        try {
            connection.close();
            connection = null;
            log.info("Connection discard");
        } catch (Exception e) {
            log.error("Error: something wrong happened while disconnecting from DB");
        }
    }
    public void dropDB() {
        executeSQLQuery(
                "DROP DATABASE IF EXISTS test123",
                "Database has been successfully dropped",
                "Database hasn't been dropped");
    }

    public void createDB() {
        executeSQLQuery(
                "CREATE DATABASE test123",
                "Database has been successfully created",
                "Database hasn't been created");
    }

    public void createTables() {
        executeSQLQuery(
                "CREATE TABLE NODES " +
                "(ID BIGINT PRIMARY KEY NOT NULL," +
                " VERSION BIGINT," +
                " _TIMESTAMP TIMESTAMPTZ," +
                " UID BIGINT," +
                " USER_NAME TEXT," +
                " CHANGESET BIGINT," +
                ")",
                "Table Nodes has been successfully created",
                "Error: Table Nodes hasn't been created");

        executeSQLQuery(
                "CREATE TABLE WAYS " +
                "(ID BIGINT PRIMARY KEY NOT NULL," +
                " VERSION BIGINT," +
                " _TIMESTAMP TIMESTAMPTZ," +
                " UID BIGINT," +
                " USER_NAME TEXT," +
                " CHANGESET BIGINT" +
                ")",
                "Table Ways has been successfully created",
                "Error: Table Ways hasn't been created");

        executeSQLQuery(
                "CREATE TABLE RELATIONS " +
                "(ID BIGINT PRIMARY KEY NOT NULL," +
                " VERSION BIGINT," +
                " _TIMESTAMP TIMESTAMPTZ," +
                " UID BIGINT," +
                " USER_NAME TEXT," +
                " CHANGESET BIGINT" +
                ")",
                "Table Relations has been successfully created",
                "Error: Table Relations hasn't been created");
    }

    public Connection getConnection() {
        return connection;
    }

    private void executeSQLQuery(String sqlQuery,
                             String logMessage,
                             String logErrorMessage
    ) {
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(sqlQuery);
            statement.close();
            connection.commit();
            log.info(logMessage);
        } catch (SQLException exSQL) {
            log.error(logErrorMessage);
        }
    }
}
