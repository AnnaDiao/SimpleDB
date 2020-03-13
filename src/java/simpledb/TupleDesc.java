package simpledb;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.NoSuchElementException;
/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    /**主程序在下面，不是这个*/
    //多抛异常
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }//名字有可能为null，后面会处理

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    //typeAr是一共有多少个！！！ 列名是String[] fieldAr

    private int numAr;//在下面"TupleDesc"给数字
    private List<TDItem> rtTd;
    private HashMap<String,Integer> fnameToIndex;

    //interator写法
    //见https://www.cnblogs.com/guoyansi19900907/p/12131001.html
    public Iterator<TDItem> iterator() {
        //some code goes here
       return new ItrTDI();
    }
    private class ItrTDI implements Iterator<TDItem>
    {
        private int nump=0;
        public boolean hasNext()
        {
            if(nump< rtTd.size())
                return true;
            else
                return false;
        }
        public TDItem next() {
            if (!hasNext()) {
                throw new NoSuchElementException();//？
            }
            return rtTd.get(nump++);
        }
    }

    private static final long serialVersionUID = 1L;        //不知道是什么

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr    一共有多少
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.注意可能名字为NULL
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("类型数组长度必须>=1");
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("fieldAr的长度需要等于typeAr");
        }
        numAr=typeAr.length;
        //rtTd =new TDItem[numAr];//换成List
        rtTd=new ArrayList<>(numAr);
        fnameToIndex=new HashMap<>();
        for(int j=0;j<numAr;j++)
        {
            rtTd.add(j,new TDItem(typeAr[j],fieldAr[j]));
            fnameToIndex.put(fieldAr[j],j);
        }

    }//初始化完成

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]); //转回1
    }//没有名字的初始化

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numAr;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=this.numAr)
            throw new NoSuchElementException();
        return rtTd.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if(i<0||i>=this.numAr)
            throw new NoSuchElementException();
        return rtTd.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    //@dzq
    //这里的搜索可改进！
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if(name==null)
            throw new NoSuchElementException();
        /*
        for (int i = 0; i < rtTd.size(); i++) {
            String temp= rtTd.get(i).fieldName;
            if (temp != null && temp.equals(name)) {
                return i;
            }
        }*/
        if(fnameToIndex.containsKey(name))
            return fnameToIndex.get(name);
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (TDItem item : rtTd) {
            totalSize += item.fieldType.getLen();
        }
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param //td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param //td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    private TupleDesc(TDItem[] tdItems) {
        if (tdItems == null || tdItems.length == 0) {
            throw new IllegalArgumentException("数组不能为空且至少包含一个元素");
        }
        this.rtTd= Arrays.asList(tdItems);
        this.numAr = tdItems.length;
    }

    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int len1=td1.numAr;
        int len2=td2.numAr;
        TDItem[] tdItems1 = new TDItem[len1];
        td1.rtTd.toArray(tdItems1);
        TDItem[] tdItems2 = new TDItem[len2];
        td2.rtTd.toArray(tdItems2);
        TDItem[] rtItems = new TDItem[len1 + len2];
        System.arraycopy(tdItems1, 0, rtItems, 0, len1);
        System.arraycopy(tdItems2, 0, rtItems, len1, len2);
        return new TupleDesc(rtItems);

    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    //整个元组比较
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }
        if (o instanceof TupleDesc) {
            TupleDesc another = (TupleDesc) o;
            if (!(another.numFields() == this.numFields())) {
                return false;
            }

            for (int i = 0; i < this.numFields(); i++) {
                boolean nameEquals =( (rtTd.get(i).fieldName == null && another.rtTd.get(i).fieldName == null)
                        || rtTd.get(i).fieldName.equals(another.rtTd.get(i).fieldName));
                boolean typeEquals = (rtTd.get(i).fieldType.equals(another.rtTd.get(i).fieldType));
                boolean jud= (nameEquals && typeEquals);
                if (!jud) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer result = new StringBuffer();
        result.append("Fields: ");
        for (TDItem tdItem : rtTd) {
            result.append(tdItem.toString() + ", ");
        }
        return result.toString();
    }
}
