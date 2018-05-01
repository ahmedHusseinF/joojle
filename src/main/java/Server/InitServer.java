package Server;

import Classes.Searcher;
import org.bson.Document;
import java.util.Arrays;

import static spark.Spark.*;

public class InitServer {

    private Searcher searcher;

    public static void main(String[] args) {

        new InitServer();

    }

    private InitServer(){

        searcher = new Searcher();
        initRoutes();
    }

    private void initRoutes(){
        get("/q/:query", (req, res) -> {
            String query = req.params("query");
            Document responseObject = new Document();

            responseObject.append("query", query);

            String[] results = (new Searcher()).getSimilarQueries(query.split(" "));

            responseObject.append("data", Arrays.asList(results));

            return responseObject.toJson();
        });


        get("/s/:query", (req, res) -> {
            String query = req.params("query");

            Document responseObject = new Document();

            return responseObject.toJson();
        });
    }
}