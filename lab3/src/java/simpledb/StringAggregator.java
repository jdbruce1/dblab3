package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    TupleDesc agTd;
    Hashtable<Integer, Tuple> storedTuples;
    int ourKey;
    boolean grouping;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	
    	if(this.what != Op.COUNT){
    		throw new IllegalArgumentException();
    	}
    	
    	this.grouping = gbfield != Aggregator.NO_GROUPING; 
    	
    	if(this.grouping){
	    	Type[] agTypes = new Type[2];
	    	agTypes[0] = gbfieldtype;
	    	agTypes[1] = Type.INT_TYPE;
	    	String[] agStrs = new String[2];
	    	agStrs[0] = "Group By Field";
	    	agStrs[1] = what.toString();
	    	this.agTd = new TupleDesc(agTypes,agStrs);
    	}else{
    		Type[] agType = new Type[1];
    		agType[0] = Type.INT_TYPE;
    		String[] agStr = new String[1];
    		agStr[0] = what.toString();
    		this.agTd = new TupleDesc(agType,agStr);
    	}
    	
    	this.storedTuples = new Hashtable<Integer, Tuple>();
    	
    	ourKey = 0;
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	
    	Iterator<Integer> it = storedTuples.keySet().iterator();
    	
    	int currVal;
    	
    	int intAfield;
    	
    	if(this.grouping){
    		intAfield = 1;
    	}else{
    		intAfield = 0;
    	}
    	
    	Field next_match_field = null;
    	Field tup_match_field = null;
    			
    	
    	while(it.hasNext()){
    		int key = (Integer) it.next();
    		Tuple next = this.storedTuples.get(key);
    		
    		if(this.grouping){
    			next_match_field = next.getField(0);
    			tup_match_field = tup.getField(gbfield);
    		}
    		if( !this.grouping || (next_match_field.equals(tup_match_field))){
    			currVal = ((IntField)next.getField(intAfield)).getValue();
    			switch(this.what){
    			case AVG:
    				throw new IllegalArgumentException("Can't average strings");
    			case MIN:
    				throw new IllegalArgumentException("Can't min on strings");
    			case MAX:
    				throw new IllegalArgumentException("Can't max on strings");
    			case SUM:
    				throw new IllegalArgumentException("Can't sum on strings");
    			case COUNT:
    				int count = currVal + 1;
    				next.setField(intAfield, new IntField(count));
    				return;
    			case SUM_COUNT:
    				break;
    			case SC_AVG:
    				break;
    			default:
    				throw new IllegalArgumentException("This aggregation case not handled");
    			}
    		}
    	}
    	
    	Tuple initTuple = new Tuple(this.agTd);
    	
    	Field agInit;
    	
    	if(this.what == Op.COUNT){
    		agInit = new IntField(1);
    	}else{
    		agInit = tup.getField(this.afield);
    	}
    	
    	
    	if(grouping){
    		initTuple.setField(0, tup.getField(this.gbfield));
    	}
    	
    	initTuple.setField(intAfield, agInit);
    	this.storedTuples.put(ourKey++, initTuple);
    	return;
    	
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new TupleIterator(this.agTd, this.storedTuples.values());
    }

}
