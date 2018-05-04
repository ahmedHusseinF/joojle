package Classes;

import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.util.*;

public class QueryProcessor {

    private DBConnection dbConnection;
    private QuickSort sorter;


    public QueryProcessor() {
        this.dbConnection = DBConnection.getInstance();
        this.sorter = new QuickSort();
    }

    public String[] getSimilarQueries(String q) {


        HashMap<String, Document> result = dbConnection.getDocumentsByFilter(new Document(), DBConnection.SUGGESTIONS, true);

        Iterator<Map.Entry<String, Document>> iter = result.entrySet().iterator();

        while (iter.hasNext()) {
            String str = iter.next().getKey();

            if (!str.startsWith(q))
                iter.remove();
        }


        Document[] suggestions = result.values().toArray(new Document[result.values().size()]);

        ArrayList<String> resSuggs = new ArrayList<>();

        for (Document suggestion : suggestions) {
            resSuggs.add(suggestion.getString("word"));
        }

        return (resSuggs.toArray(new String[resSuggs.size()]));
    }

    public Document[] getSearchResults(String[] sepQueries) {

        // word, its document
        HashMap<String, Document> sameWordResults = new HashMap<>();
        HashMap<String, Document> otherWordResults = new HashMap<>();

        // overall rank and its url
        HashMap<Double, Document> ranksMap = new HashMap<>();

        // common urls url string and its document
        HashMap<String, Document> tempCommonUrls = new HashMap<>();
        HashMap<String, Document> commonUrls = new HashMap<>();

        if (sepQueries.length == 0)
            return null;

        for (String q : sepQueries) {
            HashMap<String, Document> res = dbConnection.getDocumentsByFilter(Filters.text(q), DBConnection.INDEXED_WORDs, true);

            if (res.size() > 0) {

                for (Map.Entry<String, Document> el : res.entrySet()) {
                    if (q.equals(el.getKey())) {
                        sameWordResults.put(el.getKey(), el.getValue());
                    }
                    else
                        otherWordResults.put(el.getKey(), el.getValue());
                }

            }
        }

        sameWordResults.forEach((singleKey, singleValue) -> {
            @SuppressWarnings("unchecked")
            ArrayList<Document> urls = singleValue.get("urls", ArrayList.class);

            for (Document url : urls) {

                if (tempCommonUrls.containsKey(url.getString("url")) && !commonUrls.containsKey(url.getString("url"))) {
                    commonUrls.put(url.getString("url"), url);
                    tempCommonUrls.remove(url.getString("url"));
                }

                if (!tempCommonUrls.containsKey(url.getString("url")) && !commonUrls.containsKey(url.getString("url"))) {
                    tempCommonUrls.put(url.getString("url"), url);
                }

                HashMap<String, Document> theURL = dbConnection.getDocumentsByFilter(Filters.eq("url", url.getString("url")), DBConnection.FETCHED_URLs, false);
                double pageRank = theURL.values().iterator().next().getDouble("rank");
                double wordRank = url.getDouble("wordRank");

                double overAllRank = pageRank * wordRank;

                ranksMap.put(overAllRank, theURL.values().iterator().next());
            }
        });

        otherWordResults.forEach((singleKey, singleValue) -> {
            @SuppressWarnings("unchecked")
            ArrayList<Document> urls = singleValue.get("urls", ArrayList.class);

            for (Document url : urls) {

                if (tempCommonUrls.containsKey(url.getString("url")) && !commonUrls.containsKey(url.getString("url"))) {
                    commonUrls.put(url.getString("url"), url);
                    tempCommonUrls.remove(url.getString("url"));
                }

                if (!tempCommonUrls.containsKey(url.getString("url")) && !commonUrls.containsKey(url.getString("url"))) {
                    tempCommonUrls.put(url.getString("url"), url);
                }

                HashMap<String, Document> theURL = dbConnection.getDocumentsByFilter(Filters.eq("url", url.getString("url")), DBConnection.FETCHED_URLs, false);
                double pageRank = theURL.values().iterator().next().getDouble("rank") * 0.4;
                double wordRank = url.getDouble("wordRank");

                double overAllRank = pageRank * wordRank;

                ranksMap.put(overAllRank, theURL.values().iterator().next());
            }
        });

        if (ranksMap.size() < 2) {
            return ranksMap.values().toArray(new Document[ranksMap.size()]);
        }

        if (commonUrls.size() > 0 && sepQueries.length > 1) {
            // for multiple words in query
            // filter out commen words to sort them after this if
            Iterator<Map.Entry<Double, Document>> iter = ranksMap.entrySet().iterator();

            while (iter.hasNext()) {
                Document d = iter.next().getValue();

                if (!commonUrls.containsKey(d.getString("url")))
                    iter.remove();
            }

        }

        TreeMap<Double, Document> sorted = (TreeMap<Double, Document>) sort(ranksMap);

        // array list of documents
        return sorted.values().toArray(new Document[sorted.size()]);
    }

    private Map<Double, Document> sort(Map map) {

        @SuppressWarnings("unchecked")
        Map<Double, Document> treeMap = new TreeMap<>(
                Comparator.reverseOrder()
        );

        treeMap.putAll(map);

        return treeMap;

    }

    public void storeSearchSuggestion(String word) {
        dbConnection.insertIntoCollection(new Document().append("word", word), DBConnection.SUGGESTIONS);
    }

}
