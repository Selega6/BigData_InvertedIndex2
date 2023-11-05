package org.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SqliteReader implements DatabaseReader {
    private final Connection connection;

    public SqliteReader() throws SQLException {
        this.connection = DriverManager.getConnection("jdbc:sqlite:datamart.db");
    }

    @Override
    public List<Book> readBooks(String sql) throws SQLException {
        List<Book> allBooks = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(sql)) {

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String author = resultSet.getString("author");
                String title = resultSet.getString("title");
                Book book = new Book(id, author, title);
                allBooks.add(book);
            }
        }
        return allBooks;
    }

    @Override
    public List<Word> readWords(String sql) throws SQLException {
        List<Word> words = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(sql)) {

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String label = resultSet.getString("label");
                Word word = new Word(id, label);
                words.add(word);
            }
        }
        return words;
    }

    @Override
    public List<Associate> readAssociations(String sql) throws SQLException {
        List<Associate> associations = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(sql)) {

            while (resultSet.next()) {
                int wordId = resultSet.getInt("wordId");
                int bookId = resultSet.getInt("bookId");
                int count = resultSet.getInt("count");
                Associate associate = new Associate(wordId, bookId, count);
                associations.add(associate);
            }
        }
        return associations;
    }

    @Override
    public boolean update(String sql, String author, String title, int id) throws SQLException {
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, author);
            pstmt.setString(2, title);
            pstmt.setInt(3, id);
            int affectedRows = pstmt.executeUpdate();
            return affectedRows > 0;
        }
    }


    @Override
    public boolean delete(String sql, int id) throws SQLException {
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setInt(1, id);
            int affectedRows = pstmt.executeUpdate();
            return affectedRows > 0;
        }
    }

    @Override
    public int create(String sql, String author, String title) throws SQLException {
        int generatedId;

        connection.setAutoCommit(false);

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, author);
            pstmt.setString(2, title);
            int affectedRows = pstmt.executeUpdate();

            if (affectedRows == 0) {
                throw new SQLException("Creating the record failed, no rows affected.");
            }

            try (Statement lastIdStmt = connection.createStatement();
                 ResultSet rs = lastIdStmt.executeQuery("SELECT last_insert_rowid()")) {
                if (rs.next()) {
                    generatedId = rs.getInt(1);
                } else {
                    throw new SQLException("Creating the record failed, no ID obtained.");
                }
            }
        } catch (Exception e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }

        return generatedId;
    }
    public void closeConnection() throws SQLException {
        if (this.connection != null && !this.connection.isClosed()) {
            this.connection.close();
        }
    }
    public Connection getConnection() {
        return connection;
    }
}
