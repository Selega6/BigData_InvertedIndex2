package org.example;

import com.google.gson.Gson;
import spark.Request;
import spark.Response;

import java.sql.SQLException;
import java.util.List;

import static spark.Spark.*;

public class SparkWebService implements APISource{
    private final DataManager dataManager;
    private final Gson gson = new Gson();
    public APISource startServer() {
        port(4567);
        return this;
    }
    public SparkWebService() throws SQLException {
        this.dataManager = new DataManager();
    }
    public void start() {
        get("/api/books", this::getBooksRequest);
        get("/api/words", this::getWordsRequest);
        get("/api/associations", this::getAssociationsRequest);
        post("/api/books", this::createBookRequest);
        put("/api/books/:id", this::updateBookRequest);
        delete("/api/books/:id", this::deleteBookRequest);
        get("/api/search", (request, response) -> {//http://localhost:4567/api/search?query={word}
            String query = request.queryParams("query");
            if (query == null || query.trim().isEmpty()) {
                response.status(400);
                return "Query parameter 'query' is required";
            }

            try {
                List<Book> results = dataManager.searchBooks(query);
                response.type("application/json");
                return gson.toJson(results);
            } catch (SQLException e) {
                response.status(500);
                return "Internal Server Error: " + e.getMessage();
            }
        });

        exception(Exception.class, (exception, request, response) -> {
            exception.printStackTrace();
            response.status(500);
            response.body("Internal Server Error");
        });


    }
    @Override
    public void stopServer() {
        stop();
    }
    private Object getBooksRequest(Request request, Response response) {
        try {
            List<Book> books = dataManager.getAllBooks();
            response.type("application/json");
            return gson.toJson(books);
        } catch (SQLException e) {
            e.printStackTrace();
            response.status(500);
            return "Internal Server Error: " + e.getMessage();
        }
    }
    private Object getWordsRequest(Request request, Response response) {
        try {
            List<Word> words = dataManager.getAllWords();
            response.type("application/json");
            return gson.toJson(words);
        } catch (SQLException e) {
            e.printStackTrace();
            response.status(500);
            return "Internal Server Error: " + e.getMessage();
        }
    }

    private Object getAssociationsRequest(Request request, Response response) {
        try {
            List<Associate> associations = dataManager.getAllAssociations();
            response.type("application/json");
            return gson.toJson(associations);
        } catch (SQLException e) {
            e.printStackTrace();
            response.status(500);
            return "Internal Server Error: " + e.getMessage();
        }
    }

    private Object createBookRequest(Request request, Response response) {
        try {
            Book newBook = gson.fromJson(request.body(), Book.class);

            int bookId = dataManager.createBook(newBook);

            response.status(201);
            response.type("application/json");
            return gson.toJson(bookId);
        } catch (Exception e) {
            response.status(500);
            return "Error interno del servidor: " + e.getMessage();
        }
    }

    private Object updateBookRequest(Request request, Response response) {
        try {
            int bookId = Integer.parseInt(request.params(":id"));

            Book updatedBook = gson.fromJson(request.body(), Book.class);

            boolean success = dataManager.updateBook(bookId, updatedBook);

            if (success) {
                response.status(200);
                response.type("application/json");
                return "Libro actualizado correctamente";
            } else {
                response.status(404);
                return "Libro no encontrado";
            }
        } catch (Exception e) {
            response.status(500);
            return "Error interno del servidor: " + e.getMessage();
        }
    }

    private Object deleteBookRequest(Request request, Response response) {
        try {
            int bookId = Integer.parseInt(request.params(":id"));

            boolean success = dataManager.deleteBook(bookId);

            if (success) {
                response.status(200);
                return "Libro eliminado correctamente";
            } else {
                response.status(404);
                return "Libro no encontrado";
            }
        } catch (Exception e) {
            response.status(500);
            return "Error interno del servidor: " + e.getMessage();
        }
    }
}