package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.asList(this.items).iterator();
    }

    private static final long serialVersionUID = 1L;
    public final TDItem[] items;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        this.items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            this.items[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

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
        items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            items[i] = new TDItem(typeAr[i], "anonymous");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.length;
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
        if (i < 0 || i > this.numFields() - 1) {
            throw new NoSuchElementException("Index Out of Range");
        } else {
            return this.items[i].fieldName;
        }
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
        if (i < 0 || i >= this.numFields()) {
            throw new NoSuchElementException("Index Out of Range");
        } else {
            return items[i].fieldType;
        }
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
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < this.items.length; i++) {
            if (items[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No such field name.");

    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem field : this.items) {
            size += field.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] fieldType = new Type[td1.numFields() + td2.numFields()];
        List<Type> typeList = getAllFieldsType(td1);
        typeList.addAll(getAllFieldsType(td2));
        typeList.toArray(fieldType);

        String[] fieldName = new String[td1.numFields() + td2.numFields()];
        List<String> stringList = getAllFieldsName(td1);
        stringList.addAll(getAllFieldsName(td2));
        stringList.toArray(fieldName);

        return new TupleDesc(fieldType, fieldName);
    }

    private static List<String> getAllFieldsName(TupleDesc td) {
        List<String> list = new LinkedList<>();
        for (int i = 0; i < td.numFields(); i++) {
            list.add(td.getFieldName(i));
        }
        return list;
    }

    private static List<Type> getAllFieldsType(TupleDesc td) {
        List<Type> list = new LinkedList<>();
        for (int i = 0; i < td.numFields(); i++) {
            list.add(td.getFieldType(i));
        }
        return list;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param td
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object td) {
        // some code goes here
        if (!this.getClass().isInstance(td) || td == null) {
            return false;
        }
        TupleDesc tupleDesc = (TupleDesc) td;
        if (this.numFields() != tupleDesc.numFields()) return false;
        for (int i = 0; i < this.numFields(); i++) {
            if (this.getFieldType(i) != tupleDesc.getFieldType(i)) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int hashcode = 0;
        List<String> names = getAllFieldsName(this);
        for (String str : names) {
            hashcode += str.hashCode();
        }
        return hashcode;
        // throw new UnsupportedOperationException("unimplemented");
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
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < this.numFields(); i++) {
            stringBuilder.append("\"" + this.getFieldType(i) + "\"" + "(" + this.getFieldName(i) + ")  ");
        }
        return stringBuilder.toString();
    }
}
