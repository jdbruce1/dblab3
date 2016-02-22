package simpledb;

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
    
    
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    TupleDesc agTd;
    Hashtable<Integer, Tuple> storedTuples;
    Hashtable<Integer, Integer> numIncluded;
    int ourKey;
    boolean grouping;
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	
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
    	
        storedTuples = new Hashtable<Integer, Tuple>();
        numIncluded =  new  Hashtable<Integer, Integer>();

        ourKey = 0;
    	 
    }
    

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	
    	//search for tuple with same groupbyfield
    	Iterator<Integer> it = storedTuples.keySet().iterator();
    	
    	int currVal;
    	int newVal;
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
    		Tuple next = storedTuples.get(key);
    		if(grouping){
    			next_match_field = next.getField(0);
    			tup_match_field = tup.getField(gbfield);
    		}
    		if( !this.grouping || (next_match_field.equals( tup_match_field))){
    			newVal = ((IntField)tup.getField(afield)).getValue();
    			currVal = ((IntField)next.getField(intAfield)).getValue();
    			switch(what){
    				case AVG:
    					int currCount = numIncluded.get(key);
    					numIncluded.put(key, currCount+1);
    					/*int currCount = numIncluded.get(key);
    					int newAvg = (currCount * currVal + newVal)/ (currCount+1);
    					numIncluded.put(key, currCount+1);
    					next.setField(intAfield, new IntField(newAvg));
    					return;*/
    				case SUM:
    					int sum = currVal + newVal;
    					next.setField(intAfield, new IntField(sum));
    					return;
    				case COUNT:
    					int count = currVal + 1;
    					next.setField(intAfield, new IntField(count));
    					return;
    				case MIN:
    					if(newVal < currVal){
    						next.setField(intAfield, new IntField(newVal));
    					}
    					return;
    				case MAX:
    					if(newVal > currVal){
    						next.setField(intAfield, new IntField(newVal));
    					}
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
    	if(grouping){
    		initTuple.setField(0, tup.getField(gbfield));
    	}
    	
    	if(this.what == Op.COUNT){
    		initTuple.setField(intAfield, new IntField(1));
    	}else{
    		initTuple.setField(intAfield, tup.getField(this.afield));
    	}
    	this.storedTuples.put(ourKey, initTuple);
    	this.numIncluded.put(ourKey++, 1);
    	return;
    	    	
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        
    	if(this.what == Op.AVG){
    		Iterator <Integer> keyIt = this.storedTuples.keySet().iterator();
    		int curKey;
    		Tuple curTup;
    		Tuple newTup;
    		int sum;
    		int count;
    		int intAfield;
    		int avg;
        	if(this.grouping){
        		intAfield = 1;
        	}else{
        		intAfield = 0;
        	} 
    		
        	Set<Tuple> tupSet = new HashSet<Tuple>();
        	
    		while(keyIt.hasNext()){
    			curKey = keyIt.next();
    			curTup = this.storedTuples.get(curKey);
    			sum = ((IntField)curTup.getField(intAfield)).getValue();
    			count = this.numIncluded.get(curKey);
    			avg = sum / count;
    			newTup = new Tuple(this.agTd);
    			newTup.setField(intAfield, new IntField(avg));
    			if(grouping){
    				newTup.setField(0, curTup.getField(0));
    			}
    			tupSet.add(newTup);
    			//curTup.setField(intAfield, new IntField(avg));
    		}
    		
    		
    		return new TupleIterator(this.agTd, tupSet);
    	}
    	
    	
        return new TupleIterator(this.agTd, this.storedTuples.values()); 
    }

}
