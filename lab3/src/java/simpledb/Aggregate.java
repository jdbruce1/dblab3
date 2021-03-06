package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    //internal fields 
    DbIterator child;
    int afield;
    int gbfield;
    Aggregator.Op aop;
    Aggregator intAg;
    boolean is_open;
    DbIterator intIt;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    	this.child = child;
    	this.afield = afield;
    	this.gbfield = gfield;
    	this.aop = aop;
    	
    	TupleDesc childTd = this.child.getTupleDesc();
    	
    	Type atype = childTd.getFieldType(this.afield);
    	
    	Type gbtype = null;
    	
    	if(gfield != Aggregator.NO_GROUPING){
    		gbtype = childTd.getFieldType(this.gbfield);
    	}
    	
    	try {
			child.open();
		} catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TransactionAbortedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	if(atype == Type.INT_TYPE){
    		this.intAg = new IntegerAggregator(this.gbfield,gbtype,this.afield,this.aop);
    	}else if(atype == Type.STRING_TYPE){
    		this.intAg = new StringAggregator(this.gbfield,gbtype,this.afield,this.aop);
    	}else{
    		throw new IllegalArgumentException("Unknown Type");
    	}
    	
    	this.is_open = false;
    	
    	try {
			while(this.child.hasNext()){
				this.intAg.mergeTupleIntoGroup(this.child.next());
			}
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (DbException e) {
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			e.printStackTrace();
		}
    	
    	this.intIt = this.intAg.iterator();
    	
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    	return this.gbfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    	if(this.gbfield >= 0){
    		return this.intIt.getTupleDesc().getFieldName(gbfield);
    	}else{
    		return null;
    	}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
    	return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	
    	return this.intIt.getTupleDesc().getFieldName(1);
    	
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
    	return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	if(is_open){
    		throw new DbException("Aggregate is already open");
    	}else{
    		is_open = true;
    		super.open();
    		
    		
    		this.intIt.open();
    	}
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
    	if(is_open && this.intIt.hasNext()){
    		return this.intIt.next();
    	}else{
    		if(!this.intIt.hasNext()){
    			return null;
    		}
    		throw new DbException("Aggregate is not open");
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
    	if(is_open){
    		super.close();
    		super.open();
    		this.close();
    		this.open();
    	}else{
    		throw new DbException("This aggregate is not open");
    	}
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    	return this.child.getTupleDesc();
    }

    public void close() {
	// some code goes here
    	if(this.is_open){
    		super.close();
    		this.is_open = false;
    		this.intIt.close();
    	}else{
    		//throw new DbException("This aggregate is not open");
    	}
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
