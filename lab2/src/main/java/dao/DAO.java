package dao;
import java.util.ArrayList;

public interface DAO<T> {
    void insertStatement(T el);
    void insertPreparedStatement(T el);
    void insertBatch(ArrayList<T> list);
}