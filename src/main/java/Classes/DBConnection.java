package Classes;

import com.mongodb.client.FindIterable;
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class DBConnection {

    private static DBConnection instance = null;

    private final MongoDatabase database;

    private static final String DB_NAME = "joojle";
    public static final String SEED_LIST = "SeedList";
    public static final String INDEXED_URLs = "IndexedURLs";
    public static final String DISALLOWED_URLs = "DisallowedURLs";



    private DBConnection() {
        MongoClient client = new MongoClient(new MongoClientURI("mongodb://admin:EDVeunnibidriz2@ds157528.mlab.com:57528/joojle"));
        this.database = client.getDatabase(DB_NAME);
    }

    public Document getLatestEntry(String collection) {
        synchronized (database) {
            return this.database.getCollection(collection).findOneAndDelete(new Document());
        }
    }

    public static DBConnection getInstance() {
        if(instance == null) {
            instance = new DBConnection();
        }

        return instance;
    }

    public boolean isThisObjectExist(Document obj, String collection){

        synchronized (database){
            return database.getCollection(collection).count(obj) > 0;
        }

    }

    public boolean updateCollection(Document obj, Document filter, String collection){
        synchronized (database){
            try {

                MongoCollection mongoCollection = this.database.getCollection(collection);
                mongoCollection.updateOne(filter, obj);
                return true;

            }catch (Exception e){

                System.out.println(e.getMessage());
                return false;

            }

        }
    }

    public void insertIntoCollection(Document obj, String collection) {
        synchronized (database) {
            try {

                MongoCollection<Document> mongoCollection = this.database.getCollection(collection);
                mongoCollection.insertOne(obj);


            } catch (Exception e) {

                System.out.println(e.getMessage());


            }

        }
    }

}
