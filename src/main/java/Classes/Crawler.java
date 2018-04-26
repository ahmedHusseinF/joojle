package Classes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class Crawler extends Thread {

    private DBConnection DBConn;

    private final String USER_AGENT = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";

    public static void main(String args[]) {

        int threadsLimit;

        Scanner reader = new Scanner(System.in);  // Reading from System.in
        System.out.println("Enter a number for maximum threads: ");

        // Scans the next token of the input as an int.
        threadsLimit = reader.nextInt();

        if (threadsLimit <= 0) {
            threadsLimit = 5;
            System.out.println("Default Threads limit (5) is being used");
        }

        //once finished
        reader.close();

        List<Crawler> crawlers = new ArrayList<>();

        for (int i = 0; i < threadsLimit; i++) {
            Crawler aCrawler = new Crawler();
            crawlers.add(aCrawler);
            aCrawler.start();
        }


        crawlers.forEach(crawler -> {
            try {
                crawler.join();
            } catch (InterruptedException e) {
                e.printStackTrace(System.out);
            }
        });


    }

    private Crawler() {
        super();
        this.DBConn = DBConnection.getInstance();
    }


    private String getLatestSeed() {

        return DBConn.getLatestEntry(DBConnection.SEED_LIST).get("url", String.class);

    }

    private boolean robotsSafe(String url) throws IOException {
        URL urlObj = new URL(url);

        String hostId = urlObj.getProtocol() + "://" + urlObj.getHost()
                + (urlObj.getPort() > -1 ? ":" + urlObj.getPort() : "");

        String robots = this.sentGETRequest(hostId + "/robots.txt");

        BaseRobotRules rules;


        if (robots == null) {

            rules = new SimpleRobotRules(SimpleRobotRules.RobotRulesMode.ALLOW_ALL);

        } else {

            SimpleRobotRulesParser robotParser = new SimpleRobotRulesParser();
            rules = robotParser.parseContent(hostId, robots.getBytes(StandardCharsets.UTF_8),
                    "text/plain", USER_AGENT);

        }

        return rules.isAllowed(url);
    }

    private String sentGETRequest(String url) throws IOException {

        URL uri = new URL(url);


        HttpURLConnection conn = (HttpURLConnection) uri.openConnection();

        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000); //set timeout to 5 seconds
        conn.setRequestProperty("User-agent", USER_AGENT);

        int responseCode = conn.getResponseCode();

        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in =
                    new BufferedReader(new InputStreamReader(conn.getInputStream()));

            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);   // append all stream bytes to make the whole page
            }

            in.close(); // close the stream to tie up loose ends

            return response.toString();

        } else if (responseCode == HttpURLConnection.HTTP_MOVED_PERM || responseCode == HttpURLConnection.HTTP_MOVED_TEMP) {

            String newLocationOfUrl = conn.getHeaderField("location"); // standard response header if the url has been moved

            return this.sentGETRequest(newLocationOfUrl); // recursevly try to send get request to new location

        } else {
            return null;
        }
    }

    private void parseDocument(String html, String normalizedUrl) throws IOException, URISyntaxException {

        org.jsoup.nodes.Document doc = Jsoup.parse(html);

        Elements links = doc.body().select("a[href]"); // get all links that points to other pages, not buttons

        Element title = doc.head().selectFirst("title");
        if (title == null)
            title = doc.head().selectFirst("meta[name='og:title']");


        Element description = doc.head().selectFirst("meta[name='description']");
        if (description == null)
            description = doc.head().selectFirst("meta[name='og:description']");
        if (description == null)
            description = doc.head().selectFirst("meta[name='twitter:description']");


        Element keywordsSeparated = doc.head().selectFirst("meta[name='keywords']");
        if (keywordsSeparated == null)
            keywordsSeparated = doc.head().selectFirst("meta[name='og:keywords']");
        if (keywordsSeparated == null)
            keywordsSeparated = doc.head().selectFirst("meta[name='twitter:keywords']");


        String bodyContent = doc.body().toString();

        List<Document> linksDocuments = new ArrayList<>();

        for (Element link : links) {
            String eachUrl = link.attr("abs:href");

            if (eachUrl.isEmpty())
                continue;

            String newNormalizedUrl = new URI(eachUrl).normalize().toString();

            Bson equalNormalizedUrl = Filters.eq("url", newNormalizedUrl);

            if (DBConn.isThisObjectExist(equalNormalizedUrl, DBConnection.SEED_LIST))
                continue;

            if (DBConn.isThisObjectExist(equalNormalizedUrl, DBConnection.FETCHED_URLs)){
                HashMap<String, Document> fetchedUrl = DBConn.getDocumentsByFilter(equalNormalizedUrl, DBConnection.FETCHED_URLs);

                int currentUrlInLinks = fetchedUrl.get(0).get("inLinks", Integer.class);

                //fetchedUrl.get(0).put("inLinks", ++currentUrlInLinks);

                Bson updatedPortion = Updates.set("inLinks", ++currentUrlInLinks);

                DBConn.replaceDocumentByFilter(updatedPortion, equalNormalizedUrl, DBConnection.FETCHED_URLs);

                continue;
            }


            String protocol = new URL(eachUrl).getProtocol();

            if (!protocol.equals("http") && !protocol.equals("https"))
                continue;



            linksDocuments.add(new Document().append("url", newNormalizedUrl));
        }

        if(linksDocuments.size() > 0)
            // insert one big chunk and not one at a time in the loop
            DBConn.insertManyIntoCollection(linksDocuments, DBConnection.SEED_LIST);

        // insert the indexed document
        DBConn.insertIntoCollection(
                new Document()
                        .append("url", normalizedUrl)
                        .append("outLinks", links.size())
                        .append("inLinks", 0)
                        .append("body", bodyContent)
                        .append("indexed", false)
                        .append("description", description != null ? description.attr("content") : "")
                        .append("title", title != null ? title.text() : "")
                        .append("keywords", keywordsSeparated != null ? Arrays.asList(keywordsSeparated.attr("content").split("\\s*,\\s*")) : "")

                , DBConnection.FETCHED_URLs
        );

    }

    @Override
    public void run() {
        while (true) {
            this.crawl();
        }
    }

    private void crawl() {
        try {

            String url = this.getLatestSeed();

            String normalizedUrl = new URI(url).normalize().toString();

            boolean isAllowed = this.robotsSafe(normalizedUrl);

            if (isAllowed) {
                String res = this.sentGETRequest(normalizedUrl);

                if (res == null || res.isEmpty())
                    throw new Exception("Empty page Document at url " + url);


                parseDocument(res, normalizedUrl);

                System.out.println("Processed this link " + normalizedUrl);

            } else {

                System.out.println("url: " + normalizedUrl + " isn't robots safe");
            }


        } catch (Exception e) {
            System.out.println("generic exception: " + e.toString());
            System.out.println("exception message: " + e.getMessage());
            System.out.println("exception cause: " + e.getCause());
            System.out.print("exception trace: ");
            e.printStackTrace(System.out);
            System.out.println();
        }
    }
}
