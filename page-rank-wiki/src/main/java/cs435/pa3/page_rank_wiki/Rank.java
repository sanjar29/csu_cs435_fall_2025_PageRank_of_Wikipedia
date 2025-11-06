package cs435.pa3.page_rank_wiki;

import java.io.Serializable;

public class Rank implements Serializable {

    private final long pageId;
    private final double rankValue;

    public long getPageId() {
        return pageId;
    }

    public double getRankValue() {
        return rankValue;
    }

    public Rank(long pageId, double rankValue) {
        super();
        this.pageId = pageId;
        this.rankValue = rankValue;
    }

}
