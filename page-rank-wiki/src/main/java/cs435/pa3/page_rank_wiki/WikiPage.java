package cs435.pa3.page_rank_wiki;

import java.io.Serializable;

public class WikiPage implements Serializable {

    private long pageId;
    private final String title;
    private final Links links;
    private Long[] pageRank;

    public long getPageId() {
        return pageId;
    }

    public String getTitle() {
        return title;
    }

    public Links getLinks() {
        return links;
    }

    public Long[] getPageRank() {
        return pageRank;
    }

    public void setPageRank(Long[] pageRank) {
        this.pageRank = pageRank;
    }

    public WikiPage() {
        super();
        this.pageId = 0L;
        this.title = "";
        this.links = new Links();
        this.pageRank = new Long[1];
    }

    public WikiPage(long pageId, String title, Links links) {
        super();
        this.pageId = pageId;
        this.title = title;
        this.links = links;
        this.pageRank = new Long[1];
    }

}
