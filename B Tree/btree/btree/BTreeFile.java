package btree;

import java.io.IOException;

import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import diskmgr.Page;

public class BTreeFile extends IndexFile implements GlobalConst {
	private BTreeHeaderPage headerPage;	
	private PageId  headerPageId;
	private String  dbname; 
	
	public BTreeFile(String filename) throws HFDiskMgrException{
		headerPageId = get_file_entry(filename); //get header id from disk
	    headerPage= new  BTreeHeaderPage(headerPageId); //creat a HFPage and pin it with headerPageID
	    dbname = new String(filename);
	}
	
	 public BTreeFile(String filename,int keytype,int keysize,int delete_fashion) throws HFDiskMgrException{

		 headerPageId = get_file_entry(filename);
		 if( headerPageId==null){ //file not exist
			 headerPage = new  BTreeHeaderPage(); 
			 headerPageId = headerPage.getPageId();
			 add_file_entry(filename, headerPageId);
//			 headerPage.set_magic0(MAGIC0);
			 headerPage.set_rootId(new PageId(INVALID_PAGE));
			 headerPage.set_keyType((short)keytype);    
			 headerPage.set_maxKeySize(keysize);
			 headerPage.set_deleteFashion( 0 ); ///<<<<<<<<<
//			 headerPage.set_keyType(NodeType.BTHEAD); ///<<<<<<<<
		 }else {
			 headerPage = new BTreeHeaderPage( headerPageId );  
		 } 
	     dbname=new String(filename);
	}
	 
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////
	 
	 
	 public void close() throws ReplacerException, PageUnpinnedException, HashEntryNotFoundException, InvalidFrameNumberException{
		 if ( headerPage!=null) { //not destroyed
			 SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			 headerPage=null;
		 }  
	 }
	 
	 
	 public void destroyFile() throws HFBufMgrException, HFDiskMgrException, ConstructPageException, IOException, IteratorException{
		 if( headerPage != null) { //not destroyed yet!
			 PageId pid = headerPage.get_rootId();
			 if( pid.pid != INVALID_PAGE) /* have a root */destroyFileRecursive(pid);
			 unpinPage(headerPageId,false);
			 freePage(headerPageId);      
			 delete_file_entry(dbname);
			 headerPage=null;
		 }
	 }
	 
	 private void  destroyFileRecursive(PageId pageId) throws HFBufMgrException, ConstructPageException, IOException, IteratorException{
		 Page page = new Page(); 
		 pinPage(pageId,page,false);
		 
		 int keyType = headerPage.get_keyType();
		 BTSortedPage sortedPage = new BTSortedPage(page, keyType); //pageID<<
			      
		 if (sortedPage.getType() == NodeType.INDEX) {
			 BTIndexPage indexPage = new BTIndexPage(page, keyType); //pageID<<
			 
			 RID rid = new RID();
			 PageId childId; 
			 KeyDataEntry entry;
			 for (entry = indexPage.getFirst(rid) ; entry!=null ; entry = indexPage.getNext(rid)){ 
				 childId = ((IndexData)(entry.data)).getData();
				 destroyFileRecursive(childId);
			 }
			 
		 }else { //leafPage 
			 unpinPage(pageId,false);
			 freePage(pageId);
		 }      
	 }
	 
	 
	 public void insert(KeyClass key,RID rid) throws KeyTooLongException, KeyNotMatchException, IOException, 
	 	ConstructPageException, LeafInsertRecException, HFBufMgrException, NodeNotMatchException, ConvertException{
		 
		 //Check the Key
		 if (BT.getKeyLength(key) > headerPage.get_maxKeySize())
			 throw new KeyTooLongException(null, "");
		 if (key instanceof StringKey) {
			 if (headerPage.get_keyType() != AttrType.attrString) 
				 throw new KeyNotMatchException(null, "");
		 }else if (key instanceof IntegerKey) {
			 if (headerPage.get_keyType() != AttrType.attrInteger)	
				 throw new KeyNotMatchException(null, "");
		 }else	
			 throw new KeyNotMatchException(null, "");
		 
		 
		 if (headerPage.get_rootId().pid == INVALID_PAGE) { //no root page
			 //creat root page (LeafPage)
			 BTLeafPage newRootPage = new BTLeafPage(headerPage.get_keyType());
			 PageId newRootPageId = newRootPage.getCurPage();
			 //set next and prev
			 newRootPage.setNextPage(new PageId(INVALID_PAGE));
			 newRootPage.setPrevPage(new PageId(INVALID_PAGE));
			 //insert the record
			 newRootPage.insertRecord(key, rid);
			 //unpin and flush
			 unpinPage(newRootPageId, true); //dirty
			 //update the header page
			 updateHeader(newRootPageId);
			 return; //end
		 }
		 
		 KeyDataEntry newRootEntry = insertRecursive(key, rid, headerPage.get_rootId());
		 
		 
	 }
	 
	 
	 private KeyDataEntry insertRecursive(KeyClass key, RID rid, PageId currentPageId) throws HFBufMgrException, IOException, 
	 	ConstructPageException, KeyNotMatchException, NodeNotMatchException, ConvertException, LeafInsertRecException {
		 
		 KeyDataEntry upEntry;
		 
		 Page curPage = new Page();
		 pinPage(currentPageId, curPage, false); //pin the root page
		 BTSortedPage currentPage = new BTSortedPage(curPage, headerPage.get_keyType()); //currentPage >> root page
		 
		 //current page (Leaf or Index)
		 if (currentPage.getType() == NodeType.INDEX) { // recurse and then split if necessary
			 
			 BTIndexPage currentIndexPage = new BTIndexPage(curPage,headerPage.get_keyType()); //currentIndexPage >> root page
			 PageId nextPageId = currentIndexPage.getPageNoByKey(key);
			 unpinPage(currentPageId,false);
			 upEntry = insertRecursive(key, rid, nextPageId);
			
			 
		 }else if (currentPage.getType() == NodeType.LEAF) {
			 BTLeafPage currentLeafPage = new BTLeafPage(curPage,headerPage.get_keyType()); 
			 
			 //check avilable space
			 if (currentLeafPage.available_space() >= BT.getKeyDataLength(key,NodeType.LEAF)) {//no split
					currentLeafPage.insertRecord(key, rid);
					unpinPage(currentPageId, true);
					return null;
			 }
			 
			 
		 }


		 
		 return null;
	 }

	
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////////////////////////

	


	private void updateHeader(PageId newRootPageId) {
		// TODO Auto-generated method stub
		
	}

	private void pinPage(PageId pageno, Page page, boolean emptyPage)throws HFBufMgrException {
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: pinPage() failed");
		}
	 } 
		
	 private void unpinPage(PageId pageno, boolean dirty)
				throws HFBufMgrException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: unpinPage() failed");
		}
	}

	private void freePage(PageId pageno) throws HFBufMgrException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: freePage() failed");
		}
	}

	private PageId newPage(Page page, int num) throws HFBufMgrException {
		PageId tmpId = new PageId();
		try {
			tmpId = SystemDefs.JavabaseBM.newPage(page, num);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Heapfile.java: newPage() failed");
		}
		return tmpId;
	}

	private PageId get_file_entry(String filename) throws HFDiskMgrException {
		PageId tmpId = new PageId();
		try {
			tmpId = SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: get_file_entry() failed");
		}
		return tmpId;
	}

	private void add_file_entry(String filename, PageId pageno)throws HFDiskMgrException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: add_file_entry() failed");
		}
	}

	private void delete_file_entry(String filename) throws HFDiskMgrException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,"Heapfile.java: delete_file_entry() failed");
		}
	}


	public BTLeafPage findRunStart(KeyClass key, RID curRid) {
		// TODO Auto-generated method stub
		return null;
	}

	public BTFileScan new_scan(KeyClass lowkey, KeyClass hikey) {
		// TODO Auto-generated method stub
		return null;
	}

	public void traceFilename(String string) {
		// TODO Auto-generated method stub
		
	}

	public BTreeHeaderPage getHeaderPage() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean Delete(KeyClass key, RID rid) {
		// TODO Auto-generated method stub
		return false;
	}
}
