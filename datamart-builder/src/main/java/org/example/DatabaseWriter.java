package org.example;

import java.sql.*;

public class DatabaseWriter implements Datamart{

    private final Connection connection;

    public DatabaseWriter() throws SQLException{
        String url ="jdbc:sqlite:datamart.db";
        connection = DriverManager.getConnection(url);
        initDatabase();
    }


    private static final String BOOK =
            "CREATE TABLE IF NOT EXISTS book(" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "author TEXT, " +
                    "title TEXT)";
    private static final String WORD =
            "CREATE TABLE IF NOT EXISTS word(" +
                    "id INTEGER UNIQUE, " +
                    "label TEXT PRIMARY KEY)";
    private static final String ASSOCIATION =
            "CREATE TABLE IF NOT EXISTS associate(" +
                    "wordId INTEGER, " +
                    "bookId INTEGER, " +
                    "count INTEGER," +
                    "PRIMARY KEY (wordId, bookId))";

    @Override
    public void initDatabase() throws SQLException {
        connection.createStatement().execute(BOOK);
        connection.createStatement().execute(WORD);
        connection.createStatement().execute(ASSOCIATION);

    }

    @Override
    public void addBook(Book book) throws SQLException {
        connection.createStatement().execute(SqliteWriter.insertBookStatementOf(book));
    }

    @Override
    public void addWord(Word word) throws SQLException {
        connection.createStatement().execute(SqliteWriter.insertWordStatementOf(word));
    }

    @Override
    public void addAssociation(Associate associate) throws SQLException {
        connection.createStatement().execute(SqliteWriter.insertAssociateStatementOf(associate));
    }

    @Override
    public void deleteTable(String sql) throws SQLException {
        connection.createStatement().execute(sql);
    }

    @Override
    public Word findWordByLabel(String label) throws SQLException {
        String query = "SELECT * FROM word WHERE label = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, label);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String retrievedLabel = resultSet.getString("label");
                    return new Word(id, retrievedLabel);
                }
            }
        }
        return null;
    }

    public int getMaxId() {
        int maxId = 0;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT MAX(id) FROM word")) {

            if (rs.next()) {
                maxId = rs.getInt(1);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return maxId;
    }
}
