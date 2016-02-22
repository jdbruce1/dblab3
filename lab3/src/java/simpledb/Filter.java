package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    
    //global variables
    Predicate p;
    DbIterator child;
   
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	this.p = p;
    	this.child = child;	
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here  	
        return child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException{
        // some code goes here
    	this.child.open();
    	super.open();   
    }

  
    public void close() {
        // some code goes here
    	super.close();
    	this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	this.child.rewind();
    }
   

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
    TransactionAbortedException, DbException {
    	// some code goes here

    	Tuple theNext;
    	
    	//iterate through tuples in child
    	while(child.hasNext() ){
    		theNext = this.child.next();
    		
    		//if true for the filter then return it
    		if (p.filter(theNext) == true){
    			return theNext;
    		}
    	}

    	//if never found a matching tuple, return null
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] children_array = new DbIterator[1];
    	children_array[0] = this.child;
        return children_array;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	this.child = children[0];
    }

}
