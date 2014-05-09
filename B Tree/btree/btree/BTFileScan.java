package btree;

import java.io.*;

import global.*;
import heap.*;

public class BTFileScan extends IndexFileScan implements GlobalConst {

	BTreeFile bfile;
	String treeFilename;
	BTLeafPage leafPage;
	RID curRid;
	boolean didfirst;
	boolean deletedcurrent;

	KeyClass endkey;
	int keyType;
	int maxKeysize;

	public KeyDataEntry get_next() throws ScanIteratorException {

		KeyDataEntry entry;
		PageId nextpage;
		try {
			if (leafPage == null)
				return null;

			if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
				didfirst = true;
				deletedcurrent = false;
				entry = leafPage.getCurrent(curRid);
			} else {
				entry = leafPage.getNext(curRid);
			}

			while (entry == null) {
				nextpage = leafPage.getNextPage();
				unpinPage(leafPage.getCurPage(), true);
				if (nextpage.pid == INVALID_PAGE) {
					leafPage = null;
					return null;
				}

				leafPage = new BTLeafPage(nextpage, keyType);

				entry = leafPage.getFirst(curRid);
			}

			if (endkey != null)
				if (BT.keyCompare(entry.key, endkey) > 0) {
					unpinPage(leafPage.getCurPage(), false);
					leafPage = null;
					return null;
				}

			return entry;
		} catch (Exception e) {
			e.printStackTrace();
			throw new ScanIteratorException();
		}
	}

	public void delete_current() throws ScanDeleteException {

		KeyDataEntry entry;
		try {
			if (leafPage == null) {
				System.out.println("No Record to delete!");
				throw new ScanDeleteException();
			}

			if ((deletedcurrent == true) || (didfirst == false))
				return;

			entry = leafPage.getCurrent(curRid);
			unpinPage(leafPage.getCurPage(), false);
			bfile.Delete(entry.key, ((LeafData) entry.data).getData());
			leafPage = bfile.findRunStart(entry.key, curRid);

			deletedcurrent = true;
			return;
		} catch (Exception e) {
			e.printStackTrace();
			throw new ScanDeleteException();
		}
	}

	public int keysize() {
		return maxKeysize;
	}

	public void DestroyBTreeFileScan() throws IOException,
			bufmgr.InvalidFrameNumberException, bufmgr.ReplacerException,
			bufmgr.PageUnpinnedException, bufmgr.HashEntryNotFoundException,
			HFBufMgrException {
		if (leafPage != null) {
			unpinPage(leafPage.getCurPage(), true);
		}
		leafPage = null;
	}

	private void unpinPage(PageId pageno, boolean dirty)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Scan.java: unpinPage() failed");
		}

	}
}
