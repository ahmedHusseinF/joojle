package Classes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import crawlercommons.robots.BaseRobotRules;
import crawlercommons.robots.SimpleRobotRules;
import crawlercommons.robots.SimpleRobotRulesParser;
import org.bson.Document;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class Crawler extends Thread {

    private DBConnection DBConn;

    private final String USER_AGENT = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";

    Crawler() {
        super();
        this.DBConn = DBConnection.getInstance();
    }


    private String getLatestSeed() {

        return this.DBConn.getLatestEntry(DBConnection.SEED_LIST).get("url", String.class);

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

        } else if(responseCode == HttpURLConnection.HTTP_MOVED_PERM || responseCode == HttpURLConnection.HTTP_MOVED_TEMP) {

            String newLocationOfUrl = conn.getHeaderField("location"); // standard response header if the url has been moved

            return this.sentGETRequest(newLocationOfUrl); // recursevly try to send get request to new location

        } else {
            return null;
        }
    }

    private void parseDocument(String html, String url) throws IOException {

        org.jsoup.nodes.Document doc = Jsoup.parse(html);

        Elements links = doc.body().select("a[href]"); // get all links that points to other pages, not buttons

        Element title = doc.head().selectFirst("title");
        if(title == null)
            title = doc.head().selectFirst("meta[name='og:title']");


        Element description = doc.head().selectFirst("meta[name='description']");
        if(description == null)
                description = doc.head().selectFirst("meta[name='og:description']");
        if(description == null)
            description = doc.head().selectFirst("meta[name='twitter:description']");


        Element keywordsSeparated = doc.head().selectFirst("meta[name='keywords']");
        if(keywordsSeparated == null)
            keywordsSeparated = doc.head().selectFirst("meta[name='og:keywords']");
        if(keywordsSeparated == null)
            keywordsSeparated = doc.head().selectFirst("meta[name='twitter:keywords']");


        String bodyContent = doc.body().toString();

        for (Element link : links) {
            String eachUrl = link.attr("abs:href");

            if(eachUrl.isEmpty())
                continue;

            if(DBConn.isThisObjectExist(new Document().append("url", eachUrl), DBConnection.SEED_LIST))
                continue;

            if(DBConn.isThisObjectExist(new Document().append("url", eachUrl), DBConnection.INDEXED_URLs))
                continue;

            if(new URL(eachUrl).getProtocol().equals("mailto"))
                continue;

            DBConn.insertIntoCollection(new Document().append("url", eachUrl), DBConnection.SEED_LIST);

        }


        DBConn.insertIntoCollection(
                new Document()
                        .append("url", url)
                        .append("outLinks", links.size())
                        .append("body", bodyContent)
                        .append("inLinks", 0)
                        .append("description", description != null ? description.attr("content") : "")
                        .append("title", title != null ? title.text() : "")
                        .append("keywords", keywordsSeparated != null ? Arrays.asList(keywordsSeparated.attr("content").split("\\s*,\\s*")) : "")

                , DBConnection.INDEXED_URLs
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

            boolean isAllowed = this.robotsSafe(url);

            if (isAllowed) {
                String res = this.sentGETRequest(url);

                if( res == null || res.isEmpty() )
                    throw new Exception("Empty page Document at url " + url);


                this.parseDocument(res, url);

                System.out.println("Processed this link " + url);
            }


        } catch (IOException e) {
            System.out.println("IOException: " + e.toString());
        } catch (Exception e) {
            System.out.println("generic exception: " + e.toString());
            System.out.println("exception cause: " + e.getCause());
            System.out.print("exception trace: ");
            e.printStackTrace(System.out);
        }
    }
}
