package Classes;

import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;

public class Searcher  {

    private DBConnection dbConnection;

    public static void main(String args[]){

    }

    public Searcher(){
        this.dbConnection = DBConnection.getInstance();
    }

    public String[] getSimilarQueries(String[] q){
        Bson filters = Filters.eq("");

        for (String word: q) {
            filters = Filters.or(filters, Filters.eq(word));
        }

        ArrayList<Document> result = dbConnection.getDocumentsByFilter(filters, DBConnection.INDEXED_WORDs);

        return ((String[]) result.toArray());
    }

}
