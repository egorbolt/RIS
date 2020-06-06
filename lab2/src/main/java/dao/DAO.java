package dao;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;

public interface DAO<T> {
    void insertStatement(T el, Statement statement);
    void insertPreparedStatement(T el, PreparedStatement statement);
    void insertBatch(ArrayList<T> list, PreparedStatement statement);
}