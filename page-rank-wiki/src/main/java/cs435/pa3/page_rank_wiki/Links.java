package cs435.pa3.page_rank_wiki;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class Links implements Serializable {

    private final Long pageId;
    private final List<Long> linkedPages;

    public Long getPageId() {
        return pageId;
    }

    public List<Long> getLinkedPages() {
        return linkedPages;
    }

    public Links() {
        super();
        this.pageId = 0L;
        this.linkedPages = new LinkedList<>();
    }

    public Links(Long page, List<Long> linkedPages) {
        super();
        this.pageId = page;
        this.linkedPages = linkedPages;
    }

    public int getCount() {
        return linkedPages.size();
    }
}
