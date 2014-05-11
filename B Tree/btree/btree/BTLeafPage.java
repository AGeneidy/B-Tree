package btree;

import java.io.IOException;

import global.PageId;
import global.RID;
import diskmgr.Page;

public class BTLeafPage extends BTSortedPage {
	public BTLeafPage(int arg1) throws ConstructPageException, IOException {
		// TODO Auto-generated constructor stub
		super(arg1);
		this.setType(NodeType.LEAF);
	}

	public BTLeafPage(Page arg0, int arg1) throws IOException {
		super(arg0, arg1);
		this.setType(NodeType.LEAF);
		// TODO Auto-generated constructor stub
	}

	public BTLeafPage(PageId arg0, int arg1) throws ConstructPageException,
			IOException {
		super(arg0, arg1);
		this.setType(NodeType.LEAF);
		// TODO Auto-generated constructor stub
	}

	public RID insertRecord(KeyClass key, RID dataRid)
			throws LeafInsertRecException {
		KeyDataEntry KDEntry;
		try {
			KDEntry = new KeyDataEntry(key, dataRid);
			return insertRecord(KDEntry);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new LeafInsertRecException(e, " ");
		}
	}

	public KeyDataEntry getFirst(RID rid) throws IteratorException {
		KeyDataEntry KDEntry;
		try {
			rid.pageNo = getCurPage();
			rid.slotNo = 0;
			if (getSlotCnt() <= 0)
				return null;
			KDEntry = BT.getEntryFromBytes(getpage(), getSlotOffset(0),
					getSlotLength(0), keyType, NodeType.LEAF);
			return KDEntry;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new IteratorException(e, " ");
		}

	}

	public KeyDataEntry getNext(RID rid) throws IteratorException {
		KeyDataEntry KDEntry;
		try {
			int i = ++rid.slotNo;
			if (getSlotCnt() <= i)
				return null;
			KDEntry = BT.getEntryFromBytes(getpage(), getSlotOffset(i),
					getSlotLength(i), keyType, NodeType.LEAF);
			return KDEntry;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new IteratorException(e, " ");
		}

	}

	public KeyDataEntry getCurrent(RID rid) throws IteratorException {
		rid.slotNo--;
		return getNext(rid);
	}

	public boolean delEntry(KeyDataEntry dEntry) throws LeafDeleteException {
		RID Rid = new RID();
		KeyDataEntry entry;
		try {
			for (entry = getFirst(Rid); entry != null; entry = getNext(Rid)) {
				if (entry.equals(dEntry)) {
					if (!super.deleteSortedRecord(Rid))
						throw new LeafDeleteException(null,
								"Delete record failed");

					return true;
				}
			}
			return false;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new LeafDeleteException(e, "");
		}
	}

}
