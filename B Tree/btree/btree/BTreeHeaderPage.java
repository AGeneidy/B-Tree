package btree;

import global.PageId;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFPage;

import java.io.IOException;

import diskmgr.Page;

public class BTreeHeaderPage extends HFPage {

	public BTreeHeaderPage(PageId headerPageId) throws ConstructPageException {
		// TODO Auto-generated constructor stub
		super();
		try {
			pinPage(headerPageId, this, false);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new ConstructPageException();
		}
	}

	public BTreeHeaderPage(Page page) {
		// TODO Auto-generated constructor stub
		super(page);
	}

	public BTreeHeaderPage() throws ConstructPageException {
		super();
		try {
			Page apage = new Page();
			PageId id = SystemDefs.JavabaseBM.newPage(apage, 1);
			if (id == null)
				throw new ConstructPageException(null, "new page failed");
			this.init(id, apage);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new ConstructPageException(null,
					"Construct header page failed");
		}
		;

	}

	public PageId get_rootId() throws IOException {
		// TODO Auto-generated method stub
		return this.getNextPage();
	}

	public short get_keyType() throws IOException {
		// TODO Auto-generated method stub
		return (short) this.getSlotLength(3);
	}

	public void set_rootId(PageId pageId) throws IOException {
		// TODO Auto-generated method stub
		this.setNextPage(pageId);

	}

	public PageId getPageId() throws IOException {
		// TODO Auto-generated method stub
		return this.getCurPage();
	}

	public void set_keyType(short keytype) throws IOException {
		// TODO Auto-generated method stub
		this.setSlot(3, (int) keytype, 0);

	}

	public void set_maxKeySize(int keysize) throws IOException {
		// TODO Auto-generated method stub
		this.setSlot(1, keysize, 0);

	}

	public void set_deleteFashion(int delete_fashion) throws IOException {
		// TODO Auto-generated method stub
		this.setSlot(2, delete_fashion, 0);

	}

	public void get_deleteFashion(int delete_fashion) throws IOException {
		// TODO Auto-generated method stub
		this.getSlotLength(2);
	}

	public int get_maxKeySize() throws IOException {
		// TODO Auto-generated method stub
		return getSlotLength(1);
	}

	private void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws HFBufMgrException {

		try {
			SystemDefs.JavabaseBM.pinPage(pageno, page, emptyPage);
		} catch (Exception e) {
			throw new HFBufMgrException(e, "Scan.java: pinPage() failed");
		}

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
