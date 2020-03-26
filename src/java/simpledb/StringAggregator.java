package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbField;
    private Type gbFieldtype;
    private int aField;
    private Op what;
    private HashMap<Field, Integer> fld2val;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!what.equals(Op.COUNT))
            throw new IllegalArgumentException("Wrong in StrAgg! Not supported!");
        gbField=gbfield;
        gbFieldtype=gbfieldtype;
        aField=afield;
        this.what=what;
        fld2val=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField tmpaField = (StringField) tup.getField(this.aField);
        Field tmpgbField = this.gbField == NO_GROUPING ? null : tup.getField(this.gbField);
        String newValue = tmpaField.getValue();
        if (tmpgbField != null && tmpgbField.getType() != this.gbFieldtype) {
            throw new IllegalArgumentException("Wrong in StrAgg! Given tuple has wrong type");
        }
        if (!this.fld2val.containsKey(tmpgbField))
            this.fld2val.put(tmpgbField, 1);
        else
            this.fld2val.put(tmpgbField, this.fld2val.get(tmpgbField) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StrArgItr(gbFieldtype,fld2val);
    }
    private class StrArgItr implements OpIterator
    {

        Map<Field,Integer> Fld2val;
        Iterator<Map.Entry<Field,Integer>> ItrFld2val;
        boolean uzPair;
        TupleDesc ItrTd;
        StrArgItr(Type gptype,HashMap<Field,Integer> fld2val)
        {

            Fld2val=fld2val;
                if (gptype == null) {
                    ItrTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});

                } else {
                    ItrTd = new TupleDesc(new Type[]{gptype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});

                }

            ItrFld2val=null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
                ItrFld2val=Fld2val.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(ItrFld2val==null)
                return false;

                return ItrFld2val.hasNext();
        }

        void setValSig(Tuple t,Field f,int val)
        {
            if (f == null) {
                t.setField(0, new IntField(val));
            } else {
                t.setField(0, f);
                t.setField(1, new IntField(val));       //。。。类型转换
            }
        }
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple rtTp=new Tuple(ItrTd);

                Map.Entry<Field, Integer> tmpEn = ItrFld2val.next();
                Field f = tmpEn.getKey();
                this.setValSig(rtTp, f, tmpEn.getValue());

            return rtTp;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {

                ItrFld2val=Fld2val.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return ItrTd;
        }

        @Override
        public void close() {
            ItrFld2val=null;
            ItrTd=null;     //WHY?
        }
    }

}

