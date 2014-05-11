package btree;

import java.io.IOException;

import btree.BTIndexPage;
import btree.BTLeafPage;
import btree.BTSortedPage;
import btree.KeyDataEntry;
import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
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
	private PageId headerPageId;
	private String dbname;

	public BTreeFile(String filename) throws HFDiskMgrException,
			ConstructPageException {
		headerPageId = get_file_entry(filename); // get header id from disk
		headerPage = new BTreeHeaderPage(headerPageId); // creat a HFPage and
														// pin it with
														// headerPageID
		dbname = new String(filename);
	}

	public BTreeFile(String filename, int keytype, int keysize,
			int delete_fashion) throws HFDiskMgrException, IOException,
			ConstructPageException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) { // file not exist
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			// headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(0); // /<<<<<<<<<
			// headerPage.set_keyType(NodeType.BTHEAD); ///<<<<<<<<
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}
		dbname = new String(filename);
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////////////////

	public void close() throws ReplacerException, PageUnpinnedException,
			HashEntryNotFoundException, InvalidFrameNumberException {
		if (headerPage != null) { // not destroyed
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	public void destroyFile() throws HFBufMgrException, HFDiskMgrException,
			ConstructPageException, IOException, IteratorException {
		if (headerPage != null) { // not destroyed yet!
			PageId pid = headerPage.get_rootId();
			if (pid.pid != INVALID_PAGE) /* have a root */
				destroyFileRecursive(pid);
			unpinPage(headerPageId, false);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void destroyFileRecursive(PageId pageId) throws HFBufMgrException,
			ConstructPageException, IOException, IteratorException {
		Page page = new Page();
		pinPage(pageId, page, false);

		int keyType = headerPage.get_keyType();
		BTSortedPage sortedPage = new BTSortedPage(page, keyType); // pageID<<

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page, keyType); // pageID<<

			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage
					.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				destroyFileRecursive(childId);
			}

		} else { // leafPage
			unpinPage(pageId, false);
			freePage(pageId);
		}
	}

	public void insert(KeyClass key, RID rid) throws KeyTooLongException,
			KeyNotMatchException, IOException, ConstructPageException,
			LeafInsertRecException, HFBufMgrException, NodeNotMatchException,
			ConvertException, DeleteRecException, InsertRecException,
			IteratorException {

		// Check the Key
		if (BT.getKeyLength(key) > headerPage.get_maxKeySize())
			throw new KeyTooLongException(null, "");
		if (key instanceof StringKey) {
			if (headerPage.get_keyType() != AttrType.attrString)
				throw new KeyNotMatchException(null, "");
		} else if (key instanceof IntegerKey) {
			if (headerPage.get_keyType() != AttrType.attrInteger)
				throw new KeyNotMatchException(null, "");
		} else
			throw new KeyNotMatchException(null, "");

		if (headerPage.get_rootId().pid == INVALID_PAGE) { // no root page

			// creat root page (LeafPage)
			BTLeafPage newRootPage = new BTLeafPage(headerPage.get_keyType());
			PageId newRootPageId = newRootPage.getCurPage();

			// set next and prev
			newRootPage.setNextPage(new PageId(INVALID_PAGE));
			newRootPage.setPrevPage(new PageId(INVALID_PAGE));

			// insert the record
			newRootPage.insertRecord(key, rid);

			// unpin and flush
			unpinPage(newRootPageId, true); // dirty

			// update the header page
			updateHeader(newRootPageId);

			return; // end
		}

		KeyDataEntry newRootEntry = insertRecursive(key, rid,
				headerPage.get_rootId());

		if (newRootEntry != null) {

			// creat and pin a newRootPage
			BTIndexPage newRootPage = new BTIndexPage(headerPage.get_keyType());
			PageId newRootPageId = newRootPage.getCurPage();

			// insert the pushed up entry in the newRootPage
			newRootPage.insertKey(newRootEntry.key,
					((IndexData) newRootEntry.data).getData());

			// set prev pointer of the newRootPage
			newRootPage.setPrevPage(headerPage.get_rootId());

			unpinPage(newRootPageId, true);

			updateHeader(newRootPageId);
		}
	}

	private KeyDataEntry insertRecursive(KeyClass key, RID rid,
			PageId currentPageId) throws HFBufMgrException, IOException,
			ConstructPageException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, LeafInsertRecException,
			DeleteRecException, IteratorException, InsertRecException {

		KeyDataEntry upEntry;

		Page curPage = new Page();
		pinPage(currentPageId, curPage, false);
		BTSortedPage currentPage = new BTSortedPage(curPage,
				headerPage.get_keyType());

		// current page (Leaf / Index)
		if (currentPage.getType() == NodeType.INDEX) { // INDEX >> recurse then
														// split if necessary

			BTIndexPage currentIndexPage = new BTIndexPage(curPage,
					headerPage.get_keyType());
			PageId childPageId = currentIndexPage.getPageNoByKey(key);

			unpinPage(currentPageId, false);

			upEntry = insertRecursive(key, rid, childPageId);

			if (upEntry == null) // no split in the prev level
				return null;

			// there is a split done in the prev level
			// push/copy up done
			// pushed nod = upEntry

			// pin current page again
			Page page = new Page();
			pinPage(currentPageId, page, false);
			currentIndexPage = new BTIndexPage(page, headerPage.get_keyType());

			// check available space in index page for pushed up key
			if (currentIndexPage.available_space() >= BT.getKeyDataLength(
					upEntry.key, NodeType.INDEX)) {
				// push up with no split
				currentIndexPage.insertKey(upEntry.key,
						((IndexData) upEntry.data).getData());
				unpinPage(currentPageId, true);
				return null;
			}

			// the is no space for pushed up key
			// split Index Page

			// creat and pin new index page
			BTIndexPage newIndexPage = new BTIndexPage(headerPage.get_keyType());
			PageId newIndexPageId = newIndexPage.getCurPage();

			KeyDataEntry tmpEntry;
			RID tempRid = new RID();

			// move all records from currentPage >> newPage
			for (tmpEntry = currentIndexPage.getFirst(tempRid); tmpEntry != null; tmpEntry = currentIndexPage
					.getFirst(tempRid)) {
				newIndexPage.insertKey(tmpEntry.key,
						((IndexData) tmpEntry.data).getData());
				currentIndexPage.deleteSortedRecord(tempRid);
			}

			// move half the records from newPage to>> currentPage
			for (tmpEntry = newIndexPage.getFirst(tempRid); (currentIndexPage
					.available_space() > newIndexPage.available_space()); tmpEntry = newIndexPage
					.getFirst(tempRid)) {
				currentIndexPage.insertKey(tmpEntry.key,
						((IndexData) tmpEntry.data).getData());
				newIndexPage.deleteSortedRecord(tempRid);
			}

			// undo the final record
			if (currentIndexPage.available_space() < newIndexPage
					.available_space()) {
				newIndexPage.insertKey(tmpEntry.key,
						((IndexData) tmpEntry.data).getData());
				currentIndexPage
						.deleteSortedRecord(new RID(currentIndexPage
								.getCurPage(), (int) currentIndexPage
								.getSlotCnt() - 1));
			}

			// put the record in the proper Page
			RID firstRid = new RID();
			tmpEntry = newIndexPage.getFirst(firstRid);

			if (BT.keyCompare(upEntry.key, tmpEntry.key) >= 0) { // insert
																	// upEntry
																	// in the
																	// new Page
				newIndexPage.insertKey(upEntry.key,
						((IndexData) upEntry.data).getData());

			} else { // upEntry (small) >> insert in the current Page
				currentIndexPage.insertKey(upEntry.key,
						((IndexData) upEntry.data).getData());

				// move one record from current page to>> new page
				int lastIndex = (int) currentIndexPage.getSlotCnt() - 1;
				tmpEntry = BT.getEntryFromBytes(currentIndexPage.getpage(),
						currentIndexPage.getSlotOffset(lastIndex),
						currentIndexPage.getSlotLength(lastIndex),
						headerPage.get_keyType(), NodeType.INDEX);
				newIndexPage.insertKey(tmpEntry.key,
						((IndexData) tmpEntry.data).getData());
				currentIndexPage.deleteSortedRecord(new RID(currentIndexPage
						.getCurPage(), lastIndex));
			}

			// delete the pushed up record and return it in upEntry
			RID deletedRid = new RID();
			upEntry = newIndexPage.getFirst(deletedRid);
			newIndexPage.deleteSortedRecord(deletedRid);

			// set prev pointer of the new page
			newIndexPage.setPrevPage(((IndexData) upEntry.data).getData());

			// unpin pages
			unpinPage(currentPageId, true);
			unpinPage(newIndexPageId, true);

			// set the pageId of the pushed up Entry to the newPageID
			((IndexData) upEntry.data).setData(newIndexPageId);

			return upEntry;

		} else if (currentPage.getType() == NodeType.LEAF) {
			BTLeafPage currentLeafPage = new BTLeafPage(curPage,
					headerPage.get_keyType());

			// check avilable space
			if (currentLeafPage.available_space() >= BT.getKeyDataLength(key,
					NodeType.LEAF)) {
				// no split
				currentLeafPage.insertRecord(key, rid);
				unpinPage(currentPageId, true);
				return null;
			}

			// ///////////////////
			// no space >> split//
			// ///////////////////

			// creat new leaf page
			BTLeafPage newLeafPage = new BTLeafPage(headerPage.get_keyType());
			PageId newLeafPageId = newLeafPage.getCurPage();

			// set next and prev pointers
			newLeafPage.setNextPage(currentLeafPage.getNextPage());
			newLeafPage.setPrevPage(currentPageId);
			currentLeafPage.setNextPage(newLeafPageId);
			// set prev of right page
			PageId rightPageId = newLeafPage.getNextPage();
			if (rightPageId.pid != INVALID_PAGE) { // right page available
				BTLeafPage rightPage = new BTLeafPage(rightPageId,
						headerPage.get_keyType());
				rightPage.setPrevPage(newLeafPageId);
				unpinPage(rightPageId, true);
			}

			KeyDataEntry tmpEntry;
			RID tempRid = new RID();

			// move all records from currentPage to>> newPage
			for (tmpEntry = currentLeafPage.getFirst(tempRid); tmpEntry != null; tmpEntry = currentLeafPage
					.getFirst(tempRid)) {
				newLeafPage.insertRecord(tmpEntry.key,
						((LeafData) (tmpEntry.data)).getData());
				currentLeafPage.deleteSortedRecord(tempRid);
			}

			// move half of records from newPage to>> currentPage
			for (tmpEntry = newLeafPage.getFirst(tempRid); newLeafPage
					.available_space() < currentLeafPage.available_space(); tmpEntry = newLeafPage
					.getFirst(tempRid)) {
				currentLeafPage.insertRecord(tmpEntry.key,
						((LeafData) tmpEntry.data).getData());
				newLeafPage.deleteSortedRecord(tempRid);
			}

			// insert the key in the proper page
			if (BT.keyCompare(key, tmpEntry.key) < 0) {
				// undo the final record
				if (currentLeafPage.available_space() < newLeafPage
						.available_space()) { // <<<<<leh a check
					newLeafPage.insertRecord(tmpEntry.key,
							((LeafData) tmpEntry.data).getData());
					currentLeafPage.deleteSortedRecord(new RID(currentLeafPage
							.getCurPage(),
							(int) currentLeafPage.getSlotCnt() - 1));
				}
				currentLeafPage.insertRecord(key, rid);
			} else
				newLeafPage.insertRecord(key, rid);

			// copy up
			tmpEntry = newLeafPage.getFirst(tempRid);
			upEntry = new KeyDataEntry(tmpEntry.key, newLeafPageId);

			// unpin Pages
			unpinPage(currentPageId, true);
			unpinPage(newLeafPageId, true);

			return upEntry;
		}

		return null;
	}

	public boolean Delete(KeyClass key, RID rid) throws IOException,
			ConstructPageException, HFBufMgrException, KeyNotMatchException,
			NodeNotMatchException, ConvertException, LeafDeleteException {

		PageId currentPageId = headerPage.get_rootId();

		if (currentPageId.pid == INVALID_PAGE) { // no root page
			return false;
		}

		Page curPage = new Page();
		pinPage(currentPageId, curPage, false);
		BTSortedPage currentPage = new BTSortedPage(curPage,
				headerPage.get_keyType()); // ???????

		while (currentPage.getType() == NodeType.INDEX) {

			// get next nodeId
			BTIndexPage currentIndexPage = new BTIndexPage(curPage,
					headerPage.get_keyType());
			PageId childPageId = currentIndexPage.getPageNoByKey(key);

			// unpin current
			unpinPage(currentPageId, false);

			currentPageId = childPageId;

			curPage = new Page();
			pinPage(currentPageId, curPage, false);
			currentPage = new BTSortedPage(curPage, headerPage.get_keyType());
		}

		// Node Type = Leaf

		BTLeafPage currentLeafPage = new BTLeafPage(curPage,
				headerPage.get_keyType());
		KeyDataEntry entry = new KeyDataEntry(key, rid);

		unpinPage(currentPageId, true);

		return currentLeafPage.delEntry(entry);

	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////////////////

	private void updateHeader(PageId newRootPageId) throws HFBufMgrException,
			IOException {

		Page page = new Page();
		pinPage(headerPageId, page, false);
		BTreeHeaderPage header = new BTreeHeaderPage(page);

		header.set_rootId(newRootPageId);

		unpinPage(headerPageId, true);
	}

	// ////////////////////////////////////////////////////////////////////////////////////////////////
	// ///////////SHORT
	// CUTS///////////////////////////////////////////////////////////////////////////
	// ////////////////////////////////////////////////////////////////////////////////////////////////

	private void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws HFBufMgrException {
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
			throw new HFDiskMgrException(e,
					"Heapfile.java: get_file_entry() failed");
		}
		return tmpId;
	}

	private void add_file_entry(String filename, PageId pageno)
			throws HFDiskMgrException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(filename, pageno);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,
					"Heapfile.java: add_file_entry() failed");
		}
	}

	private void delete_file_entry(String filename) throws HFDiskMgrException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			throw new HFDiskMgrException(e,
					"Heapfile.java: delete_file_entry() failed");
		}
	}

	public BTLeafPage findRunStart(KeyClass key, RID startRID)
			throws IOException, PinPageException, ConstructPageException,
			IteratorException, KeyNotMatchException, HFBufMgrException {
		// TODO Auto-generated method stub
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;
		pageno = headerPage.getNextPage();
		if (pageno.pid == INVALID_PAGE) {
			pageLeaf = null;
			return pageLeaf;
		}
		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());
		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startRID);
			while (curEntry != null && BT.keyCompare(curEntry.key, key) < 0) {
				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startRID);
			}
			unpinPage(pageno, false);
			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());
		}
		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());
		curEntry = pageLeaf.getFirst(startRID);
		while (curEntry == null) {
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno, false);
			if (nextpageno.pid == INVALID_PAGE)
				return null;
			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startRID);
		}
		if (key == null)
			return pageLeaf;
		while (BT.keyCompare(curEntry.key, key) < 0) {
			curEntry = pageLeaf.getNext(startRID);
			while (curEntry == null) {
				nextpageno = pageLeaf.getNextPage();
				if (nextpageno.pid == INVALID_PAGE)
					return null;
				unpinPage(pageno, false);
				pageno = nextpageno;
				page = pinPage(pageno);
				pageLeaf = new BTLeafPage(page, headerPage.get_keyType());
				curEntry = pageLeaf.getFirst(startRID);
			}
		}
		return pageLeaf;
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		// TODO Auto-generated method stub
		Page apage = new Page();
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, apage, false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new PinPageException(e, "");
		}
		return apage;
	}

	public BTFileScan new_scan(KeyClass lowkey, KeyClass hikey) {
		// TODO Auto-generated method stub
		return null;
	}

	public void traceFilename(String string) {
		// TODO Auto-generated method stub

	}

	public BTreeHeaderPage getHeaderPage() {

		return headerPage;
	}

}
