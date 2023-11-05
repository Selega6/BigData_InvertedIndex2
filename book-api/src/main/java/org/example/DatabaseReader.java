package org.example;

import java.sql.SQLException;
import java.util.List;

public interface DatabaseReader {
    List<Book> readBooks(String sql) throws SQLException;

    List<Word> readWords(String sql) throws SQLException;

    List<Associate> readAssociations(String sql) throws SQLException;

    boolean update(String sql, String author, String title, int id) throws SQLException;
    boolean delete(String sql, int id) throws SQLException;
    int create(String sql, String author, String title) throws SQLException;
}
