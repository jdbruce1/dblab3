package simpledb;

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

    public Vector<TDItem> attributes;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	return attributes.iterator();
    }

    private static final long serialVersionUID = 1L;

    public TupleDesc() {
    	attributes = new Vector<TDItem>();

    }
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
    	attributes = new Vector<TDItem>();
    	for(int i = 0; i < typeAr.length; ++i) {
    		TDItem t = new TDItem(typeAr[i], fieldAr[i]);
    		attributes.add(t);
    		
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
    	
    	attributes = new Vector<TDItem>();
    	
    	for(int i = 0; i < typeAr.length; ++i) {
    		TDItem t = new TDItem(typeAr[i], "");
    		attributes.add(t);
    		
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
    	return attributes.size();

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
    	if(i < numFields()) {
    		return  attributes.get(i).fieldName;
    	}
     	throw new NoSuchElementException();

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
    	if(i < numFields()) {
    		return  attributes.get(i).fieldType;
    	}
     	throw new NoSuchElementException();

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
    	if(name == null)  {
    		throw new NoSuchElementException();    
    	}
    	for(int i = 0; i < attributes.size(); ++i) {
    		if(name.equals(attributes.get(i).fieldName)) {
    			return i;
    		}
    	}
    	throw new NoSuchElementException();    	

    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int sum = 0;
    	for(int i = 0; i < attributes.size(); ++i) {
        	
    		sum += attributes.get(i).fieldType.getLen();
    	}
    	
    	return sum;

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
    	TupleDesc out = new TupleDesc();
    	for(int i = 0; i < td1.attributes.size(); ++i) {
    		out.attributes.add(td1.attributes.get(i));;
    	}
    	
    	for(int i = 0; i < td2.attributes.size(); ++i) {
    		out.attributes.add(td2.attributes.get(i));;
    	}
    	
    	return out;
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
    public boolean equals(Object o) {
    	if(o == null) {
    		return false;
    	}
    	if(o.getClass().equals(TupleDesc.class)) {
    		TupleDesc td = (TupleDesc) o;
    		if(td.attributes.size() != attributes.size()) {
    			return false;
    		}
    		for(int i = 0; i < attributes.size(); ++i) {
    			if(!td.attributes.get(i).fieldName.equals(attributes.get(i).fieldName) || 
    			   !td.attributes.get(i).fieldType.equals(attributes.get(i).fieldType)) {
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
        String s = new String();
        s = attributes.get(0).toString();
        for(int i = 1; i < attributes.size(); ++i) {
        	 s = s + attributes.get(i).toString();
        }
    	
        return s;
    }
}
