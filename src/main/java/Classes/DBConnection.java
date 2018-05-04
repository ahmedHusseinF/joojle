package Classes;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.HashMap;
import java.util.List;

class DBConnection {

    static final String SEED_LIST = "SeedList";
    static final String FETCHED_URLs = "FetchedURLs";
    static final String INDEXED_WORDs = "IndexedWords";
    static final String SUGGESTIONS = "Suggestions";
    private static final String DB_NAME = "joojle";
    private static DBConnection instance = null;
    private final MongoDatabase database;


    private DBConnection() {

        // hardcoded connection string to ease the portability
        // old database mongodb://admin:EDVeunnibidriz2@ds157528.mlab.com:57528/joojle // instantiate with new MongoClientURI
        MongoClient client = new MongoClient("localhost", 27017);
        database = client.getDatabase(DB_NAME);
    }

    static DBConnection getInstance() {
        if (instance == null) {
            instance = new DBConnection();
        }

        return instance;
    }

    Document getLatestEntry(String collection, boolean... indexer) {

        synchronized (database) {
            if (indexer.length == 0) // true if not coming from indexer
                return database.getCollection(collection).findOneAndDelete(new Document());
            else
                return database.getCollection(collection).find(Filters.eq("indexed", false)).first();
        }
    }

    long getCollectionSize(String collection) {
        return database.getCollection(collection).count();
    }

    boolean isThisObjectExist(Bson filter, String collection) {

        synchronized (database) {
            return database.getCollection(collection).count(filter) > 0;
        }

    }

    HashMap<String, Document> getDocumentsByFilter(Bson filter, String collection, boolean isWord) {
        synchronized (database) {
            MongoCollection<Document> mongoCollection = database.getCollection(collection);

            HashMap<String, Document> documentsFetched = new HashMap<>();

            Block<Document> accumelateDocuments = document -> {
                if(isWord)
                    documentsFetched.put(document.getString("word"), document);
                else
                    documentsFetched.put(document.getString("url"), document);
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


    void updateDocumentByFilter(Bson updated, Bson filter, String collection) {
        synchronized (database) {
            try {
                MongoCollection<Document> mongoCollection = this.database.getCollection(collection);
                UpdateResult res = mongoCollection.updateOne(filter, updated, new UpdateOptions().upsert(true));
                res.wasAcknowledged(); // for debugging purposes
            } catch (Exception e) {

                System.out.println(e.getMessage());

            }

        }
    }

    void replaceDocumentByFilter(Bson filter, Document replaceDoc, String collection) {
        synchronized (database) {
            MongoCollection<Document> mongoCollection = database.getCollection(collection);
            UpdateResult res = mongoCollection.replaceOne(filter, replaceDoc);
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
