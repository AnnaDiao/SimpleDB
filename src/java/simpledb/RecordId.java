package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.     rid 指的是tuple
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    private PageId pageId;
    private int tupleNo;
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pageId=pid;
        this.tupleNo=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if( o instanceof RecordId)
        {
            RecordId tmpRid=(RecordId) o;
            if(tmpRid.getPageId().equals(this.pageId) && tmpRid.getTupleNumber()==this.tupleNo)
                return true;
        }
        return false;
    }// 记得 没有“==”运算符的调用equals函数！

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        return (pageId.getPageNumber()+tupleNo+pageId.getTableId());
    }

}
