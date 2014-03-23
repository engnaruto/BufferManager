package bufmgr;

import java.io.IOException;
import java.util.HashMap;

import diskmgr.*;
import global.*;

public class BufMgr {
	private int numBufs;
	// private int counter;
	private byte[][] bufpool;
	private Descriptor[] bufDescr;
	private HashMap<Integer, Integer> map;
	// private HashMap<PageId, Integer> map;
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
		this.numBufs = numBufs;
		bufpool = new byte[numBufs][GlobalConst.MINIBASE_PAGESIZE];
		bufDescr = new Descriptor[numBufs];
		map = new HashMap<Integer, Integer>();
		// map = new HashMap<PageId, Integer>();
		replacement = new ReplacementPolicy(numBufs, replaceArg);

		for (int i = 0; i < bufDescr.length; i++) {
			bufDescr[i] = new Descriptor();
		}
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
	 * @throws IOException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 * @throws BufferPoolExceededException
	 */
	public void pinPage(PageId pgid, Page page, boolean emptyPage, boolean loved)
			throws BufferPoolExceededException, InvalidPageNumberException,
			FileIOException, IOException {
		int pageFrame = getFrameNum(pgid);
		// System.out.println("\n--Hash Size = " + map.size() + "\n");
		// System.out.println(map.toString());
		if (pageFrame == -1) {
			// System.out.println("--Pin unpinnedBuffers = "
			// + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
			// if (replacement.getUnpinnedBuffers() <= 0) {
			// throw new BufferPoolExceededException(null,
			// "BufferPoolExceededException");
			// }
			int freeFrame = replacement.getFreeFrame();

			if (bufDescr[freeFrame] != null
					&& bufDescr[freeFrame].getPagenumber() != null) {
				if (bufDescr[freeFrame].isDirtybit()) {
					flushPage(bufDescr[freeFrame].getPagenumber());
					bufDescr[freeFrame].setDirtybit(false);
				}
				// map.remove(bufDescr[freeFrame].getPagenumber());
				map.remove(bufDescr[freeFrame].getPagenumber().pid);
			}

			map.put(pgid.pid, freeFrame);
			Page newPage = new Page();
			SystemDefs.JavabaseDB.read_page(pgid, newPage);
			bufpool[freeFrame] = newPage.getpage();
			page.setpage(bufpool[freeFrame]);

			bufDescr[freeFrame] = new Descriptor(pgid, 1);
			// int data = Convert.getIntValue(0, page.getpage());
			// System.out
			// .println("--Pin " + pgid + "  at frame " + freeFrame
			// + "  Pin_Count "
			// + bufDescr[freeFrame].getPin_count()
			// + " Unpinned Buffers = "
			// + replacement.getUnpinnedBuffers());
			// + " Data = " + (data - pgid.pid));
		} else {

			bufDescr[pageFrame]
					.setPin_count(bufDescr[pageFrame].getPin_count() + 1);
			replacement.removeFrame(pageFrame);
			page.setpage(bufpool[pageFrame]);
//			Page newPage = new Page(bufpool[pageFrame]);
//			page.setpage(newPage.getpage());

			// int data = Convert.getIntValue(0, page.getpage());
			// System.out
			// .println("--Inc " + pgid + "  at frame " + pageFrame
			// + "  Pin_Count "
			// + bufDescr[pageFrame].getPin_count()
			// + " Unpinned Buffers = "
			// + replacement.getUnpinnedBuffers());
			// + " Data = " + (data - pgid.pid));
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
	 * @throws PageUnpinnedExcpetion
	 * @throws PageIdNotInTheBufferPoolExcpetion
	 * @throws InvalidPageNumberException
	 * @throws HashEntryNotFoundException
	 */
	public void unpinPage(PageId pgid, boolean dirty, boolean loved)
			throws PageUnpinnedExcpetion, HashEntryNotFoundException {

		int frame = getFrameNum(pgid);
		// System.out.println("--Unpin Page " + pgid + " = "
		// + map.containsKey(pgid.pid));
		// if (map.containsKey(new PageId(pgid.pid))) {
		if (map.containsKey(pgid.pid)) {

			if (bufDescr[frame].getPin_count() == 0) {
				throw new PageUnpinnedExcpetion(null, "PageUnpinnedExcpetion");
			} else {
				bufDescr[frame]
						.setPin_count(bufDescr[frame].getPin_count() - 1);

				if (bufDescr[frame].isDirtybit() == true && dirty == false) {
					bufDescr[frame].setDirtybit(true);
				} else {
					bufDescr[frame].setDirtybit(dirty);
				}

				if (bufDescr[frame].getPin_count() == 0) {
					replacement.returnFrame(frame, loved);

					// System.out.println("--Unp " + pgid + "  at frame " +
					// frame
					// // );
					// + " Unpinned Buffers = "
					// + replacement.getUnpinnedBuffers());
				} else {
					replacement.returnFrameLoved(frame, loved);
					// System.out.println("--Dec " + pgid + "  at frame " +
					// frame
					// + "  Pin_Count " + bufDescr[frame].getPin_count());
					// + " Unpinned Buffers = "
					// + replacement.getUnpinnedBuffers());
				}
			}

		} else {
			// System.out.println("--Unpin Failed Page " + pgid);
			throw new HashEntryNotFoundException(null,
					"HashEntryNotFoundException");
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
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 * @throws InvalidRunSizeException
	 * @throws OutOfSpaceException
	 * @throws BufferPoolExceededException
	 */
	public PageId newPage(Page firstPage, int howmany)
			throws OutOfSpaceException, InvalidRunSizeException,
			InvalidPageNumberException, FileIOException, DiskMgrException,
			IOException, BufferPoolExceededException {

		if (replacement.getUnpinnedBuffers() <= 0) {
			return null;
		} else {
			PageId pageId = new PageId();
			SystemDefs.JavabaseDB.allocate_page(pageId, howmany);
			// Page page = new Page();
			pinPage(pageId, firstPage, false, false);
			return pageId;
		}
	}

	/**
	 * This method should be called to delete a page that is on disk. This
	 * routine must call the method in diskmgr package to deallocate the page.
	 * 
	 * @param pgid
	 *            the page number in the database.
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 * @throws InvalidRunSizeException
	 * @throws PagePinnedExcpetion
	 * @throws PageUnpinnedExcpetion
	 * @throws PagePinnedException
	 */
	public void freePage(PageId pgid) throws PagePinnedException,
			InvalidRunSizeException, InvalidPageNumberException,
			FileIOException, DiskMgrException, IOException {

		if (map.containsKey(pgid.pid)) {
			// System.out.println("--Free unpinnedBuffers = "
			// + SystemDefs.JavabaseBM.getNumUnpinnedBuffers());
			// replacement.returnFrame(map.get(pgid.pid));
			// bufDescr[map.get(pgid.pid)].setPagenumber(null);
			// System.out.println("--Free in " + pgid);

			int frame = getFrameNum(pgid);
			if (bufDescr[frame].getPin_count() > 1) {
				throw new PagePinnedException(null,
						"bufmgr.PagePinnedException");
			} else {
				bufDescr[frame] = null;
				// map.remove(new PageId(pgid.pid));
				map.remove(pgid.pid);
				replacement.returnFrame(frame, false);
				// replacement.returnFrame(pgid.pid);
				SystemDefs.JavabaseDB.deallocate_page(pgid);
			}
			// System.out.println("--Free Page " + pgid + " Unpinned Buffers = "
			// + replacement.getUnpinnedBuffers());
		} else {
			try {
				SystemDefs.JavabaseDB.deallocate_page(pgid);
			} catch (InvalidPageNumberException e) {
				throw new InvalidPageNumberException(null,
						"InvalidPageNumberException");
			}
		}
	}

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 * 
	 * @param pgid
	 *            the page number in the database.
	 * @throws IOException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 */
	public void flushPage(PageId pgid) throws InvalidPageNumberException,
			FileIOException, IOException {

		// System.out.println("--Flush " + pgid);
		int frameNum = getFrameNum(pgid);
		if (frameNum == -1) {
			try {
				throw new PageIdNotInTheBufferPoolExcpetion(null,
						"PageIdNotInTheBufferPoolExcpetion");
			} catch (PageIdNotInTheBufferPoolExcpetion e) {
				e.printStackTrace();
			}
		} else {

			// Page newPage = new Page(bufpool[frameNum]);
			Page newPage = new Page(bufpool[frameNum]);
			// if (bufDescr[frameNum].isDirtybit()) {
			SystemDefs.JavabaseDB.write_page(pgid, newPage);
			// bufDescr[frameNum].setDirtybit(false);
			// }

		}

	}

	public void flushAllPages() throws InvalidPageNumberException,
			FileIOException, IOException {

		// System.out.println("---Flush All Pages---");
		for (int i = 0; i < numBufs; i++) {
			if (bufDescr[i].getPagenumber() != null) {
				// System.out
				// .println("--All Flush " + bufDescr[i].getPagenumber());
				// System.out.println("--Buf ID " + i + " = "
				// + bufDescr[i].toString() + " Flush All");
				flushPage(bufDescr[i].getPagenumber());
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
		// System.out.println("--Get " + pageId);
		// System.out.println(map.toString());
		if (map.containsKey(pageId.pid)) {
			return map.get(pageId.pid);
		} else {
			return -1;
		}
	}
}
