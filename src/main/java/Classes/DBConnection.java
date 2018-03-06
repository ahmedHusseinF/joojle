package Classes;

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class DBConnection {

    private static DBConnection instance = null;

    private MongoDatabase db;

    private DBConnection() {
        MongoClient client = new MongoClient(new MongoClientURI("mongodb://admin:EDVeunnibidriz2@ds157528.mlab.com:57528/"));
        this.db = client.getDatabase("joojle");
    }

    public Document getLatestEntry(String collection) {
        return this.db.getCollection(collection).findOneAndDelete(new Document());
    }

    public static DBConnection getInstance() {
        if(instance == null) {
            instance = new DBConnection();
        }
        return instance;
    }

    public void insertIntoCollection(Document object, String collection) {

        MongoCollection dbCollection = this.db.getCollection(collection);
        dbCollection.insertOne(object);

    }

}
