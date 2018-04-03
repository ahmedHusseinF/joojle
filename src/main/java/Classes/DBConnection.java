package Classes;

import com.mongodb.Block;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

class DBConnection {

    private static DBConnection instance = null;

    private final MongoDatabase database;

    private static final String DB_NAME = "joojle";
    static final String SEED_LIST = "SeedList";
    static final String FETCHED_URLs = "FetchedURLs";
    static final String INDEXED_URLs = "IndexedURLs";


    private DBConnection() {
        MongoClient client = new MongoClient(new MongoClientURI("mongodb://admin:EDVeunnibidriz2@ds157528.mlab.com:57528/joojle"));
        this.database = client.getDatabase(DB_NAME);
    }

    Document getLatestEntry(String collection, boolean... indexer) {

        synchronized (database) {
            if (indexer.length == 0) // true if not coming from indexer
                return database.getCollection(collection).findOneAndDelete(new Document());
            else
                return database.getCollection(collection).find(Filters.eq("indexed", false)).first();
        }
    }


    static DBConnection getInstance() {
        if (instance == null) {
            instance = new DBConnection();
        }

        return instance;
    }

    boolean isThisObjectExist(Bson filter, String collection) {

        synchronized (database) {
            return database.getCollection(collection).count(filter) > 0;
        }

    }

    ArrayList<Document> getDocumentsByFilter(Bson filter, String collection) {
        synchronized (database) {
            MongoCollection<Document> mongoCollection = database.getCollection(collection);

            ArrayList<Document> documentsFetched = new ArrayList<>();

            Block<Document> accumelateDocuments = new Block<Document>() {
                @Override
                public void apply(final Document document) {
                    documentsFetched.add(document);
                }
            };

            mongoCollection.find(filter).forEach(accumelateDocuments);

            return documentsFetched;
        }
    }

    void insertManyIntoCollection(List<Document> docs, String collection) {
        try {

            MongoCollection<Document> mongoCollection = this.database.getCollection(collection);
            mongoCollection.insertMany(docs);

        } catch (Exception e) {

            System.out.println(e.getMessage());

        }


    }


    void updateDocumentInCollection(Bson obj, Bson filter, String collection) {
        synchronized (database) {
            try {

                MongoCollection mongoCollection = this.database.getCollection(collection);
                mongoCollection.updateOne(filter, obj);

            } catch (Exception e) {

                System.out.println(e.getMessage());

            }

        }
    }

    void insertIntoCollection(Document obj, String collection) {
        try {

            MongoCollection<Document> mongoCollection = database.getCollection(collection);
            mongoCollection.insertOne(obj);

        } catch (Exception e) {

            System.out.println(e.getMessage());

        }


    }

}
