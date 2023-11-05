package org.example;

import java.sql.SQLException;
import java.util.List;

public interface Command {
    List<Book> getAllBooks() throws SQLException;
    List<Word> getAllWords() throws SQLException;
    List<Associate> getAllAssociations() throws SQLException;
}
