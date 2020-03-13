package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;


    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */

    private TupleDesc tupleDesc; //表头
    //private Field[] fields; //数据
    private List<Field> fields;//数据
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc=td;
        fields=new ArrayList<>(td.numFields());
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }//get

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    RecordId recordId;

    public RecordId getRecordId() {
        // some code goes here

        return recordId;
    }//

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(i<0||i>=tupleDesc.numFields())
            throw new IllegalArgumentException("Field索引非法");
        fields.set(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if(i<0||i>=tupleDesc.numFields())
            throw new IllegalArgumentException("Field索引非法");
        if(fields.get(i)==null)
            return null;
        else
            return fields.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        //下面这行是干嘛的
        // throw new UnsupportedOperationException("Implement this");
        StringBuffer tmpstr=new StringBuffer();
        for(int j=0;j<tupleDesc.numFields()-1;++j)
        {
            tmpstr.append(fields.get(j).toString()+'\t');
        }
        tmpstr.append(fields.get(tupleDesc.numFields()-1).toString()+'\n');
        return tmpstr.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return new ItrFie();
    }
    private class ItrFie implements Iterator<Field>
    {
        private int nump=0;
        public boolean hasNext()
        {
            if(nump<fields.size())
                return true;
            else
                return false;
        }
        public Field next() {
            if (!hasNext()) {
                throw new NoSuchElementException();//？
            }
            return fields.get(nump++);
        }
    }
    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tupleDesc=td;
        fields.clear();
        fields=new ArrayList<>(td.numFields());
        //fields=new Field[td.numFields()];
    }
}
