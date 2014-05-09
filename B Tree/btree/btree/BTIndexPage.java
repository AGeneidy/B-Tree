package btree;
import global.PageId;
import global.RID;
import java.io.IOException;
import diskmgr.Page;

public class BTIndexPage  extends BTSortedPage {

	public BTIndexPage(PageId pageno, int keyType) throws ConstructPageException, IOException{
		super(pageno, keyType);
		setType(NodeType.INDEX); //set the type of the node >> INDEX
	}
	
	public BTIndexPage(Page page, int keyType) throws ConstructPageException, IOException{
		super(page, keyType);
		setType(NodeType.INDEX);
	}
	
	public BTIndexPage(int keyType) throws ConstructPageException, IOException{
		super(keyType);
		setType(NodeType.INDEX);
	}
	
	///////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	
	public RID insertKey(KeyClass key, PageId pageNo) throws InsertRecException {
		KeyDataEntry entry = new KeyDataEntry(key, pageNo);
		RID rid = super.insertRecord( entry );
		return rid;
	}
	
	public PageId getPageNoByKey(KeyClass key) throws KeyNotMatchException, NodeNotMatchException, ConvertException, IOException{
		KeyDataEntry entry;
	      		
		for (int i = getSlotCnt()-1 ; i>=0 ; i--) {
			try {
				entry= BT.getEntryFromBytes( getpage(),getSlotOffset(i),getSlotLength(i), keyType, NodeType.INDEX);
				if (BT.keyCompare(key, entry.key) >= 0) return ((IndexData)entry.data).getData();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return getPrevPage();
	} 
	
	public KeyDataEntry getFirst(RID rid) throws IOException, KeyNotMatchException, NodeNotMatchException, ConvertException{
		rid.pageNo = getCurPage();
		rid.slotNo = 0;
		
		if ( getSlotCnt() == 0)  return null;
		
		KeyDataEntry entry=BT.getEntryFromBytes( getpage(),getSlotOffset(0),getSlotLength(0),keyType, NodeType.INDEX);
		return entry;
	}
	
	public KeyDataEntry getNext(RID rid) throws KeyNotMatchException, NodeNotMatchException, ConvertException{		
		rid.slotNo++;
		try {
			if ( rid.slotNo >= getSlotCnt()) return null;
			KeyDataEntry entry = BT.getEntryFromBytes(getpage(),getSlotOffset(rid.slotNo), getSlotLength(rid.slotNo),keyType, NodeType.INDEX);
			return entry;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public PageId getLeftLink() throws IOException{
		return getPrevPage(); 
	}
	
	public void setLeftLink(PageId left) throws IOException{
		setPrevPage(left); 
	}
}