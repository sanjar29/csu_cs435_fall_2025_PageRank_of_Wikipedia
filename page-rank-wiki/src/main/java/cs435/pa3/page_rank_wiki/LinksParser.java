package cs435.pa3.page_rank_wiki;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.Function;

public class LinksParser implements Function<String, Links> {

    public LinksParser() {
        super();
    }

    @Override
    public Links call(String v1) throws Exception {
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
        
        Links links = new Links(pageNumber, linkedPages);
        return links;
    }

}
