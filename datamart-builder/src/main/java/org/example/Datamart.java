package org.example;

import java.sql.SQLException;

public interface Datamart {
    void initDatabase() throws SQLException;
    void addBook(Book book) throws SQLException;
    void addWord(Word word) throws SQLException;
    void addAssociation(Associate associate) throws SQLException;
    void deleteTable(String sql) throws SQLException;

    Word findWordByLabel(String label) throws SQLException;

    int getMaxId();
}
