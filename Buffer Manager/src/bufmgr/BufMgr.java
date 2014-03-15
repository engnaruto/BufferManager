package bufmgr;

import java.io.IOException;
import java.util.HashMap;

import diskmgr.*;
import global.*;

public class BufMgr {

	private byte[][] bufpool;
	private Descriptor[] bufDescr;
	private HashMap<PageId, Integer> map;
	private ReplacementPolicy replacement;

	/**
	 * Create the BufMgr object Allocate pages (frames) for the buffer pool in
	 * main memory and make the buffer manager aware that the replacement policy
	 * is specified by replaceArg (i.e. FIFO, LRU, MRU, love/hate)
	 * 
	 * @param numbufs
	 *            number of buffers in the buffer pool
	 * @param replaceArg
	 *            name of the buffer replacement policy
	 * */
	public BufMgr(int numBufs, String replaceArg) {
		bufpool = new byte[numBufs][GlobalConst.MINIBASE_PAGESIZE];
		bufDescr = new Descriptor[numBufs];
		map = new HashMap<PageId, Integer>();
		replacement = new ReplacementPolicy(numBufs, replaceArg);
	}

	/**
	 * Pin a page First check if this page is already in the buffer pool. If it
	 * is, increment the pin_count and return pointer to this page. If the
	 * pin_count was 0 before the call, the page was a replacement candidate,
	 * but is no longer a candidate. If the page is not in the pool, choose a
	 * frame (from the set of replacement candidates) to hold this page, read
	 * the page (using the appropriate method from diskmgr package) and pin it.
	 * Also, must write out the old page in chosen frame if it is dirty before
	 * reading new page. (You can assume that emptyPage == false for this
	 * assignment.)
	 * 
	 * @param pgid
	 *            page number in the minibase.
	 * @param page
	 *            the pointer point to the page.
	 * @param emptyPage
	 *            true (empty page), false (non­empty page).
	 */
	public void pinPage(PageId pgid, Page page, boolean emptyPage, boolean loved) {
		int pageFrame = getFrameNum(pgid);

		if (pageFrame == -1) {
			int freeFrame = replacement.getFreeFrame();

			if (bufDescr[freeFrame].dirtybit) {
				flushPage(bufDescr[freeFrame].pagenumber);
				bufDescr[freeFrame].dirtybit = false;
			}
			Page newPage = new Page();
			try {
				SystemDefs.JavabaseDB.read_page(pgid, newPage);
			} catch (InvalidPageNumberException e) {
				e.printStackTrace();
			} catch (FileIOException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (bufDescr[freeFrame].pagenumber != null) {
				map.remove(bufDescr[freeFrame].pagenumber);
			}
			bufpool[freeFrame] = page.getpage();
			bufDescr[freeFrame].pagenumber = pgid;
			bufDescr[freeFrame].pin_count++;
			map.put(pgid, freeFrame);
			page.setpage(bufpool[freeFrame]);
			replacement.incrementIfFIFO(freeFrame);
		} else {
			bufDescr[pageFrame].pin_count++;
			replacement.incrementIfFIFO(pageFrame);
			replacement.removeFrame(pageFrame);
			page.setpage(bufpool[pageFrame]);
		}
	}

	/**
	 * Unpin a page specified by a pageId. This method should be called with
	 * dirty == true if the client has modified the page. If so, this call
	 * should set the dirty bit for this frame. Further, if pin_count > 0, this
	 * method should decrement it. If pin_count = 0 before this call, throw an
	 * excpetion to report error. (for testing purposes, we ask you to throw an
	 * exception named PageUnpinnedExcpetion in case of error.)
	 * 
	 * @param pgid
	 *            page number in the minibase
	 * @param dirty
	 *            the dirty bit of the frame.
	 */
	public void unpinPage(PageId pgid, boolean dirty, boolean loved) {

		int frame = getFrameNum(pgid);

		if (map.containsKey(pgid)) {

			if (bufDescr[frame].pin_count == 0) {
				try {
					throw new PageUnpinnedExcpetion(null,
							"PageUnpinnedExcpetion");
				} catch (PageUnpinnedExcpetion e) {
					e.printStackTrace();
				}

			} else {
				bufDescr[frame].pin_count--;

				if (bufDescr[frame].dirtybit == true && dirty == false) {
					bufDescr[frame].dirtybit = true;
				} else {
					bufDescr[frame].dirtybit = dirty;
				}

				if (bufDescr[frame].pin_count == 0) {
					replacement.returnFrame(frame);
				} else {
					replacement.decrementIfFIFO(frame);
				}
			}
		} else {
			try {
				throw new PageIdNotInTheBufferPoolExcpetion(null,
						"PageIdNotInTheBufferPoolExcpetion");
			} catch (PageIdNotInTheBufferPoolExcpetion e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Allocate new page(s). Call DB Object to allocate a run of new pages and
	 * find a frame in the buffer pool for the first page and pin it. (This call
	 * allows a client f the Buffer Manager to allocate pages on disk.) If
	 * buffer is full, i.e., you can\t find a frame for the first page, ask DB
	 * to deallocate all these pages, and return null.
	 * 
	 * @param firstPage
	 *            the address of the first page.
	 * @param howmany
	 *            total number of allocated new pages.
	 * 
	 * @return the first page id of the new pages. null, if error.
	 */
	public PageId newPage(Page firstPage, int howmany) {
		if (replacement.getUnpinnedBuffers() == 0) {
			return null;
		} else {
			PageId pageId = new PageId();
			try {
				SystemDefs.JavabaseDB.allocate_page(pageId, howmany);
			} catch (OutOfSpaceException e) {
				e.printStackTrace();
			} catch (InvalidRunSizeException e) {
				e.printStackTrace();
			} catch (InvalidPageNumberException e) {
				e.printStackTrace();
			} catch (FileIOException e) {
				e.printStackTrace();
			} catch (DiskMgrException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			Page page = new Page();
			pinPage(pageId, page, false, false);
			return pageId;
		}
	}

	/**
	 * This method should be called to delete a page that is on disk. This
	 * routine must call the method in diskmgr package to deallocate the page.
	 * 
	 * @param pgid
	 *            the page number in the database.
	 */
	public void freePage(PageId pgid) {
		try {
			SystemDefs.JavabaseDB.deallocate_page(pgid);
		} catch (InvalidRunSizeException e) {
			e.printStackTrace();
		} catch (InvalidPageNumberException e) {
			e.printStackTrace();
		} catch (FileIOException e) {
			e.printStackTrace();
		} catch (DiskMgrException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 * 
	 * @param pgid
	 *            the page number in the database.
	 */
	public void flushPage(PageId pgid) {
		int frameNum = getFrameNum(pgid);
		if (frameNum == -1) {
			try {
				throw new PageIdNotInTheBufferPoolExcpetion(null,
						"PageIdNotInTheBufferPoolExcpetion");
			} catch (PageIdNotInTheBufferPoolExcpetion e) {
				e.printStackTrace();
			}

		} else {
			try {

				Page newPage = new Page(bufpool[frameNum]);
				// SystemDefs.JavabaseDB.allocate_page(newPageId);
				SystemDefs.JavabaseDB.write_page(pgid, newPage);
				bufDescr[frameNum].dirtybit = false;

			} catch (InvalidPageNumberException e) {
				e.printStackTrace();
			} catch (FileIOException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public int getNumUnpinnedBuffers() {
		return replacement.getUnpinnedBuffers();
	}

	// public void incrementPinCount(int frameNum) {
	// bufDescr[frameNum].pin_count++;
	// }
	//
	// public void decrementPinCount(int frameNum) {
	// bufDescr[frameNum].pin_count--;
	// }

	public int getFrameNum(PageId pageId) {
		if (map.containsKey(pageId)) {
			return map.get(pageId);
		} else {
			return -1;
		}
	}

}
