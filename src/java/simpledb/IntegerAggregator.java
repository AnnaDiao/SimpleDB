package simpledb;

//import com.sun.deploy.security.SelectableSecurityManager;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbField;
    private Type gbFieldtype;
    private int aField;
    private Op what;
    private HashMap<Field,Integer> fld2val;
    private HashMap<Field,List<Integer>> fld2ave;
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbField=gbfield;
        gbFieldtype=gbfieldtype;
        aField=afield;
        this.what=what;
        fld2val=new HashMap<>();
        fld2ave=new HashMap<>();
    }
    // 这group-by 告诉你 我要这些人在一起 和 aggregate group 给我算这些人另外一个属性的平均值
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field tmpgbFld=(gbField==NO_GROUPING?null:tup.getField(gbField));//不是afield！
        if(tmpgbFld!=null && (gbFieldtype!=tmpgbFld.getType()))
        {
            throw new IllegalArgumentException("Wrong in IntAggregator!传入类型不匹配！");
        }

        Field tmpagFld=tup.getField(aField);
        if(tmpagFld.getType()!=Type.INT_TYPE)
        {
            throw new IllegalArgumentException("Wrong in IntAggregator!计算的不是INT型！");
        }

        int rdy2add=((IntField)tmpagFld).getValue();          //看文档抬头！！！ Field有下属子类！

        //Op 在Aggregator里面
        switch (what){

            case AVG:
                if(fld2ave.containsKey(tmpgbFld))
                {
                    List<Integer> myList=fld2ave.get(tmpgbFld);
                    myList.add(rdy2add);
                    fld2ave.put(tmpgbFld,myList);
                }
                else
                {
                    List<Integer> myList=new ArrayList<>();
                    myList.add(rdy2add);
                    fld2ave.put(tmpgbFld,myList);
                }
                break;
            case MAX:
                if(fld2val.containsKey(tmpgbFld))
                {
                    fld2val.put(tmpgbFld,Math.max(fld2val.get(tmpgbFld),rdy2add));
                }
                else
                {
                    fld2val.put(tmpgbFld,rdy2add);
                }
                break;
            case MIN:
                if(fld2val.containsKey(tmpgbFld))
                {
                    fld2val.put(tmpgbFld,Math.min(fld2val.get(tmpgbFld),rdy2add));
                }
                else
                {
                    fld2val.put(tmpgbFld,rdy2add);
                }
                break;
            case SUM:
                if(fld2val.containsKey(tmpgbFld))
                {
                    fld2val.put(tmpgbFld,fld2val.get(tmpgbFld)+rdy2add);
                }
                else
                {
                    fld2val.put(tmpgbFld,rdy2add);
                }
                break;
            case COUNT:
                if(fld2val.containsKey(tmpgbFld))
                {
                    fld2val.put(tmpgbFld,fld2val.get(tmpgbFld)+1);
                }
                else
                {
                    fld2val.put(tmpgbFld,1);
                }
                break;
            case SC_AVG:

            case SUM_COUNT:
                List<Integer> tmpLin=new ArrayList<>();
                if(fld2val.containsKey(tmpgbFld))
                {
                    tmpLin=fld2ave.get(tmpgbFld);
                    //int tmpsum=tmpLin.get(0);
                    //int tmpcnt=tmpLin.get(1);
                   // tmpLin.set(0,tmpsum+rdy2add);
                   // tmpLin.set(1,tmpcnt+1);
                    tmpLin.add(rdy2add);
                    fld2ave.put(tmpgbFld,tmpLin);
                }
                else
                {
                    tmpLin.set(0,rdy2add);
                    tmpLin.set(1,1);
                    fld2ave.put(tmpgbFld,tmpLin);
                }
                break;
            default: throw new IllegalArgumentException("Wrong inIntArg! Op not support!");
        }
        return;



    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
       // throw new
       // UnsupportedOperationException("please implement me for lab2");
        return new IntArgItr(gbFieldtype,fld2val,fld2ave,what);
    }
    private class IntArgItr implements OpIterator
    {

        Map<Field,List<Integer>> Fld2ave;
        Map<Field,Integer> Fld2val;
        Iterator<Map.Entry<Field,List<Integer>>> ItrFld2ave;
        Iterator<Map.Entry<Field,Integer>> ItrFld2val;  //迭代器声名

        boolean uzPair;     //判断使用哪个迭代器
        TupleDesc ItrTd;    //返回的Tupledesc

        IntArgItr(Type gptype,HashMap<Field,Integer> fld2val,HashMap<Field,List<Integer>> fld2ave,Op what)
        {
            Fld2ave =fld2ave;
            Fld2val=fld2val;
            if(what.equals(Op.SUM_COUNT))//没有判断名字为空
            {
                if(gptype==null)
                {
                    ItrTd=new TupleDesc(new Type[]{Type.INT_TYPE,Type.INT_TYPE},new String[]{"sumVal", "countVal"});
                }
                else
                     ItrTd=new TupleDesc(new Type[]{gptype,Type.INT_TYPE,Type.INT_TYPE},new String[]{"groupVal", "sumVal", "countVal"});
            }
            else {
                if (gptype == null) {
                    ItrTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});

                } else {
                    ItrTd= new TupleDesc(new Type[] {gptype, Type.INT_TYPE}, new String[] {"groupVal", "aggregateVal"});

                }
            }
            if(what.equals(Op.SUM_COUNT)||what.equals(Op.AVG)||what.equals(Op.SC_AVG))
                uzPair=true;
            else
                uzPair=false;

            ItrFld2ave=null;
            ItrFld2val=null;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
                if(uzPair)
                {
                    ItrFld2ave=Fld2ave.entrySet().iterator();
                }
                else
                    ItrFld2val=Fld2val.entrySet().iterator();

        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(ItrFld2val==null&&ItrFld2ave==null)
                return false;

            if(uzPair)
            {
                return ItrFld2ave.hasNext();
            }
            else
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

        private int sumList(List<Integer> list) {
            int sum = 0;
            for (int i=0;i<list.size();i++)
                sum += list.get(i);
            return sum;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple rtTp=new Tuple(ItrTd);
            if(uzPair)
            {
                Map.Entry<Field,List<Integer>> tmpEn=ItrFld2ave.next();        //!TO DO

                Field f=tmpEn.getKey();
                List<Integer> pairLin=tmpEn.getValue();

                if(what.equals(Op.SUM_COUNT))//判断名字为空
                {
                    setValSig(rtTp,f,sumList(pairLin));
                    if(ItrTd.getSize()==2)
                        rtTp.setField(1,new IntField(pairLin.size()));
                    else
                        rtTp.setField(2,new IntField(pairLin.size()));      //鬼畜类型转换
                }
                if(what.equals(Op.AVG))
                {
                    int value = this.sumList(pairLin) / pairLin.size();
                    setValSig(rtTp,f, value);
                }
                if(what.equals(Op.SC_AVG))
                {

                    return rtTp;
                }
            }
            else
            {
                Map.Entry<Field, Integer> tmpEn = ItrFld2val.next();
                Field f = tmpEn.getKey();
                this.setValSig(rtTp, f, tmpEn.getValue());
            }
            return rtTp;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if(uzPair)
            {
                ItrFld2ave=Fld2ave.entrySet().iterator();
            }
            else
                ItrFld2val=Fld2val.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return ItrTd;
        }

        @Override
        public void close() {

            ItrFld2ave=null;
            ItrFld2val=null;
            ItrTd=null;

        }
    }

}
