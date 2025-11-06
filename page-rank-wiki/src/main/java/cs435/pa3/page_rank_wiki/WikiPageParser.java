package cs435.pa3.page_rank_wiki;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

public class WikiPageParser implements Function<String, WikiPage> {

    private JavaPairRDD<Long, String> titlesLookup;

    public WikiPageParser(JavaPairRDD<Long, String> titlesLookup) {
        super();
        this.titlesLookup = titlesLookup;
    }

    @Override
    public WikiPage call(String v1) throws Exception {
        String[] parts = v1.split(":");
        String pageNumberPart = parts[0].trim();
        String linksPart = parts[1].trim();

        Long pageNumber = Long.parseLong(pageNumberPart);
        List<Long> linkedPages = new LinkedList<>();

        if (linksPart.length() > 0) {
            String[] linkParts = linksPart.split(" ");

            for (String linkPartString : linkParts) {
                linkedPages.add(Long.parseLong(linkPartString));
            }
        }
        String title = lookupTitle(pageNumber);
        Links links = new Links(pageNumber, linkedPages);
        WikiPage result = new WikiPage(pageNumber, title, links);
        return result;
    }

    private String lookupTitle(Long key) throws Exception {
        String result = "";
        try {
            if (this.titlesLookup != null) {
                List<String> titles = titlesLookup.lookup(key);
                result = titles.get(0);
            }
        } catch (Exception e) {
            throw e;
        }
        return result;
    }

}
