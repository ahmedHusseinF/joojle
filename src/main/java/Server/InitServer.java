package Server;

import Classes.Searcher;
import org.bson.Document;
import java.util.ArrayList;
import java.util.Arrays;

import static spark.Spark.*;

public class InitServer {
    public static void main(String[] args) {

        new InitServer();

    }

    private InitServer(){
        initRoutes();
    }

    private void initRoutes(){
        get("/q/:query", (req, res) -> {
            String query = req.params("query");
            Document responseObject = new Document();

            responseObject.append("query", query);

            String[] results = (new Searcher()).getSimilarQueries(query.split(" "));

            //ArrayList<String> data= new ArrayList<>();

            responseObject.append("data", Arrays.asList(results));

            return responseObject.toJson();
        });



        get("/s/:query", (req, res) -> {
            String query = req.params("query");

            Document responseObject = new Document();


            res.header("Content-Length", Integer.toString(responseObject.toJson().getBytes().length));
            return responseObject.toJson();
        });
    }
}