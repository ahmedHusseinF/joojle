package Server;

import Classes.QueryProcessor;
import com.mongodb.BasicDBObject;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;

import static spark.Spark.get;

public class InitServer {

    private QueryProcessor searcher;

    private InitServer() {

        searcher = new QueryProcessor();
        initRoutes();
    }

    public static void main(String[] args) {

        new InitServer();

    }

    private void initRoutes() {

        get("/suggest/:query", (req, res) -> {
            String query = req.params("query");
            Document responseObject = new Document();

            String[] words = query.split(" ");

            responseObject.append("query", words[words.length - 1]);

            String[] results = searcher.getSimilarQueries(words[words.length - 1]);


            responseObject.append("data", Arrays.asList(results));

            return responseObject.toJson();
        });


        get("/search/:query", (req, res) -> {
            String query = req.params("query");

            Document responseObject = new Document();

            if (!query.isEmpty()) {
                responseObject.append("query", query);

                searcher.storeSearchSuggestion(query);

                Document[] results = searcher.getSearchResults(query.split(" "));

                ArrayList<Bson> resultUrlsData = new ArrayList<>();

                if (results != null) {

                    for (Document d : results) {
                        BasicDBObject data = new BasicDBObject();
                        data.append("url", d.getString("url"));
                        data.append("desc", d.getString("description"));
                        data.append("title", d.getString("title"));

                        resultUrlsData.add(data);
                    }

                    responseObject.append("results", Arrays.asList(resultUrlsData));
                }
            }

            return responseObject.toJson();
        });

    }
}