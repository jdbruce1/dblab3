package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

import javax.imageio.IIOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    
    //global variables
    TransactionId tid;
    DbIterator child;
    int tableid;
    TupleDesc td; 
    boolean flag;
    
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	
    	this.tid = t;
    	this.child = child;
    	this.tableid = tableid;
    	this.flag = false;
    	
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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
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
    			Database.getBufferPool().insertTuple(tid, this.tableid, t);
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
