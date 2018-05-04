package Classes;

import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;

public class Ranker {

    private DBConnection db;

    private Ranker() {
        this.db = DBConnection.getInstance();

        for (int j = 0; j < 100; j++) {
            this.pageRanker();
            System.out.println(j);
        }

        this.TFIDF();
    }

    public static void main(String[] args) {

        new Ranker();

    }

    private void pageRanker() {

        HashMap<String, Document> arr = db.getDocumentsByFilter(new Document(), DBConnection.FETCHED_URLs, false);
        double dampingFactor = 0.85;

        for (HashMap.Entry<String, Document> el : arr.entrySet()) {

            @SuppressWarnings("unchecked")
            ArrayList<String> inLinks = el.getValue().get("inLinks", ArrayList.class);


            double PageRank = (1 - dampingFactor);
            for (String inLink : inLinks) {
                if (arr.get(inLink) != null)
                    PageRank += dampingFactor * (arr.get(inLink).getDouble("rank") / arr.get(inLink).getInteger("outLinks"));
            }

            Document update = el.getValue();
            update.put("rank", PageRank);
            //System.out.println("This page " + el.getKey() + " got a rank of " + PageRank);

            db.replaceDocumentByFilter(Filters.eq("url", el.getKey()), update, DBConnection.FETCHED_URLs);

        }

    }


    private void TFIDF() {
        HashMap<String, Document> arr = db.getDocumentsByFilter(new Document(), DBConnection.INDEXED_WORDs, true);

        long NumberOfPages = db.getCollectionSize(DBConnection.FETCHED_URLs);

        for (HashMap.Entry<String, Document> el : arr.entrySet()) {

            int TotalWordCount = 0;

            @SuppressWarnings("unchecked")
            ArrayList<Document> Urls = el.getValue().get("urls", ArrayList.class);

            for (Document Url : Urls)
                TotalWordCount += Url.getInteger("count");


            double tempIDF = (double) NumberOfPages / (double) TotalWordCount;
            double idf = Math.log10(tempIDF);
            double tf;


            int urlsArrayCounter = 0;
            for (Document Url : Urls) {

                @SuppressWarnings("unchecked")
                int WordOccurence = Url.getInteger("count");

                @SuppressWarnings("unchecked")
                int WordsInDocument = Url.getInteger("allWordsCount");

                tf = (double) WordOccurence / (double) WordsInDocument;

                double WordRank = tf * idf;

                Urls.get(urlsArrayCounter).put("wordRank", WordRank);
                System.out.println("This word " + el.getValue().getString("word") + " got a rank of " + WordRank);

                urlsArrayCounter++;
            }

            Document update = el.getValue();
            update.put("urls", Urls);

            db.replaceDocumentByFilter(Filters.eq("word", el.getKey()), update, DBConnection.INDEXED_WORDs);
        }
    }
}
