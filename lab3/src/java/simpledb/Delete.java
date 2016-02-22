package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    
    //global variable
    TransactionId tid;
    DbIterator child;
    TupleDesc td;
    boolean flag;
    
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    	flag = false;
    	
    	//create tuple description with one int field
    	Type[] fd = new Type[1];
        fd[0] = Type.INT_TYPE;
        String[] name = new String[1];
        name[0] = "";
        this.td = new  TupleDesc(fd, name);
    	
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	super.close();
    	super.open();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here  	
 
    	Tuple t;
    	int count = 0;
    	
    	//if we have already iterated through this list before 
			//then return null because there was no space
    	if(flag) return null;
    	
    	//iterate through the tuples in child
    	while(child.hasNext()){

    		//increment the count of tuples and get next tuple
    		count++;
    		t = child.next();

    		//insert tuple using bufferpool method
    		//set flag so we know later that we have already iterated through list
    		try{
    			Database.getBufferPool().deleteTuple(tid, t);
    			flag = false;
    		}catch(IOException e){

    		}
    	}
    	
    	//create and return a new Tuple with count as the field 
    	Tuple ret = new Tuple(td);
    	ret.setField(0, new IntField(count));
    	flag = true;
    	return ret; 	
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
