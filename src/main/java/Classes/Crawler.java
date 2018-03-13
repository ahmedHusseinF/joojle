package Classes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.bson.Document;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Crawler extends Thread {

    private DBConnection db;

    public Crawler(){
        super();
        //this.db = DBConnection.getInstance();
    }


    private String getLatestSeed(){
        synchronized (db) {
            return this.db.getLatestEntry("unprocessedSeedList").get("url", String.class);
        }
    }

    private String sentGETRequest(String url) throws IOException{
        URL uri = new URL(url);

        HttpURLConnection conn = (HttpURLConnection) uri.openConnection();

        conn.setRequestMethod("GET");
        conn.setRequestProperty("User-agent", "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)");

        int responseCode = conn.getResponseCode();

        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in =
                    new BufferedReader(new InputStreamReader(conn.getInputStream()));

            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            // return result

            return response.toString();
        } else {
            throw new IOException("GET Request Failed");
        }
    }

    private void parseDocument(String html){
        org.jsoup.nodes.Document doc = Jsoup.parse(html);

        Elements els = doc.select("a[href]");

        //System.out.print(els.size());

        for (Element link : els) {
            String url = link.attr("abs:href");
            // this.db.insertIntoCollection(new Document().append("url", url), "unprocessedSeedList");
            System.out.println(url);
        }
    }

    @Override
    public void run(){
        this.crawl();
    }

    public void crawl() {
        try {
            String url = "https://www.google.com";//this.getLatestSeed();

            String res = this.sentGETRequest(url);

            //System.out.println(res);

            this.parseDocument(res);


        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
