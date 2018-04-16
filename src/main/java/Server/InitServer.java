package Server;

import org.bson.Document;
import java.util.ArrayList;

import static spark.Spark.*;

public class InitServer {
    public static void main(String[] args) {

        get("/q/:query", (req, res) -> {
            String query = req.params("query");
            Document responseObject = new Document();

            responseObject.append("query", query);

            ArrayList<String> data= new ArrayList<>();

            data.add("ahmed");
            data.add("hussein");
            data.add("fekry");
            data.add("ali");
            data.add("mohammed");

            responseObject.append("data", data);

            res.header("Content-Length", Integer.toString(responseObject.toJson().getBytes().length));

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