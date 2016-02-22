package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	
	//declare global variables
	private final TupleDesc tupleDesc;
	private final File file;
	private final int tableId;
	
    public HeapFile(File f, TupleDesc td) {
    	tupleDesc = td;
    	file = f;
    	tableId = f.getAbsolutePath().hashCode();  
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
    	return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	  HeapPageId id = (HeapPageId) pid;
          BufferedInputStream bis = null;

          try {
              bis = new BufferedInputStream(new FileInputStream(file));
              byte pageBuf[] = new byte[BufferPool.getPageSize()];
              if (bis.skip(id.pageNumber() * BufferPool.getPageSize()) != id
                      .pageNumber() * BufferPool.getPageSize()) {
                  throw new IllegalArgumentException(
                          "Unable to seek to correct place in heapfile");
              }
              int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
              if (retval == -1) {
                  throw new IllegalArgumentException("Read past end of table");
              }
              if (retval < BufferPool.getPageSize()) {
                  throw new IllegalArgumentException("Unable to read "
                          + BufferPool.getPageSize() + " bytes from heapfile");
              }
              Debug.log(1, "HeapFile.readPage: read page %d", id.pageNumber());
              HeapPage p = new HeapPage(id, pageBuf);
              return p;
          } catch (IOException e) {
              throw new RuntimeException(e);
          } finally {
              // Close the file on success or error
              try {
                  if (bis != null)
                      bis.close();
              } catch (IOException ioe) {
                  // Ignore failures closing the file
              }
          }

    }
    
    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        
    	//need to use RAF to do opposite of readPage
    	RandomAccessFile raf = new RandomAccessFile(file, "rw");
    	
    	//calculate where in the file the page belongs and write it
    	int offset = page.getId().pageNumber() * BufferPool.getPageSize();
    	raf.seek(offset);
    	raf.write(page.getPageData());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
    	return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        
    	//create null page
    	PageId newPid = null;
    	HeapPage dest = null;
    	
    	//set the flag that we've attempted insertion, so we know later
    		//if we found a page to insert or not
    	boolean flag = true;

    	//look for existing pages by iterating through all pages in this file
    	for(int i=0; i<numPages(); i++){		
			newPid = new HeapPageId(this.tableId, i);
			dest = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
			
			//check to see if there is space or not
			if (dest.getNumEmptySlots() != 0){					
				//update tuple recordId with its new location
				t.setRecordId(new RecordId(newPid, (dest.numSlots - dest.getNumEmptySlots())));
				
				//flag that we have found a file
				flag = false;
				break;
			}		
    	}
    	
    	//there is no room in existing pages, so we need to make a new page!
    	if(flag){
    		//create new PageId and from that create a new page						
    		newPid = new HeapPageId(this.tableId, numPages());		
    		dest = new HeapPage(((HeapPageId) newPid), HeapPage.createEmptyPageData());
    		
    		//update tuple recordId with its new location
    		t.setRecordId(new RecordId(newPid, 0));
    	}
    	
    	//insert tuple into the found or created page
		dest.insertTuple(t);
		
		//write the page change to this file
		this.writePage(dest);

		//return an ArrayList with the page that was updated/added
    	ArrayList<Page> arLst = new ArrayList<Page>();
    	arLst.add(dest);
    	return arLst;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        
    	//get page from bufferpool based on parameters passed
    	PageId newPid = new HeapPageId(this.tableId, t.getRecordId().getPageId().pageNumber());
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, newPid, Permissions.READ_WRITE);
    	
    	//delete the tuple from the page
		page.deleteTuple(t);
    	
    	//return an ArrayList with the page that had a tuple deleted 
    	ArrayList<Page> arLst = new ArrayList<Page>();
    	arLst.add(page);
    	return arLst;
    	
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
       return new HeapFileIterator(this, tid);
    }


}

class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    int curpgno = 0;

    TransactionId tid;
    HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open() throws DbException, TransactionAbortedException {
        curpgno = -1;
    }

    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curpgno < hf.numPages() - 1) {
            curpgno++;
            HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
            HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
                    curpid, Permissions.READ_ONLY);
            it = curp.iterator();
            if (!it.hasNext())
                it = null;
        }

        if (it == null)
            return null;
        return it.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    public void close() {
        super.close();
        it = null;
        curpgno = Integer.MAX_VALUE;
    }
}
