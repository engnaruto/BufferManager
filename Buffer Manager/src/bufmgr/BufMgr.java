package bufmgr;

import java.io.IOException;
import java.util.HashMap;

import diskmgr.*;
import global.*;

public class BufMgr {
	private int numBufs;
	private int counter;
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
		counter = 0;
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
	 */
	public void pinPage(PageId pgid, Page page, boolean emptyPage, boolean loved)
			throws InvalidPageNumberException, FileIOException, IOException {
		int pageFrame = getFrameNum(pgid);
		if (pageFrame == -1) {
			int freeFrame;
			if (counter != numBufs) {
				freeFrame = counter++;
			} else {
				freeFrame = replacement.getFreeFrame();
			}

			if (bufDescr[freeFrame].isDirtybit()) {
				flushPage(bufDescr[freeFrame].getPagenumber());
			}
			if (bufDescr[freeFrame].getPagenumber() != null
					&& map.containsKey(bufDescr[freeFrame].getPagenumber().pid)) {
				map.remove(bufDescr[freeFrame].getPagenumber().pid);
			}

			map.put(pgid.pid, freeFrame);

			Page newPage = new Page();
			// SystemDefs.JavabaseDB.read_page(pgid, page);
			SystemDefs.JavabaseDB.read_page(pgid, newPage);
			// bufpool[freeFrame] = page.getpage();
			bufpool[freeFrame] = newPage.getpage();
			page.setpage(bufpool[freeFrame]);

			bufDescr[freeFrame] = new Descriptor(pgid, 1);
			replacement.incrementIfFIFO(freeFrame);

			// System.out.println("--Pin " + pgid + "\tat frame " + freeFrame);

			// System.out.println("--Buf ID " + freeFrame + " = "
			// + bufDescr[freeFrame].toString() + " Pin");
		} else {
			SystemDefs.JavabaseDB.read_page(pgid, page);
			bufDescr[pageFrame]
					.setPin_count(bufDescr[pageFrame].getPin_count() + 1);
			// System.out.println("------------------------------");
			// replacement.incrementIfFIFO(pageFrame);
			replacement.removeFrame(pageFrame);
			// System.out.println("------------------------------");
			page.setpage(bufpool[pageFrame]);
			// System.out.println("--Inc " + pgid + "\tat frame " + pageFrame
			// + "\tPin_Count " + bufDescr[pageFrame].getPin_count());
		}
		int data = Convert.getIntValue(0, page.getpage());
		System.out.println("--Data in " + pgid.pid + " = " + (data - pgid.pid)
				+ " Pin");
		// System.out.println(map.toString());
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

		if (map.containsKey(pgid.pid)) {

			if (bufDescr[frame].getPin_count() == 0) {
				try {
					throw new PageUnpinnedExcpetion(null,
							"PageUnpinnedExcpetion");
				} catch (PageUnpinnedExcpetion e) {
					e.printStackTrace();
				}

			} else {
				bufDescr[frame]
						.setPin_count(bufDescr[frame].getPin_count() - 1);

				if (bufDescr[frame].isDirtybit() == true && dirty == false) {
					bufDescr[frame].setDirtybit(true);
				} else {
					bufDescr[frame].setDirtybit(dirty);
				}

				if (bufDescr[frame].getPin_count() == 0) {
					replacement.returnFrame(frame);

					// System.out.println("--Unp " + pgid + "\tat frame " +
					// frame);
				} else {
					// System.out.println("--Inc " + pgid + "\tat frame " +
					// frame
					// + "\tPin_Count " + bufDescr[frame].getPin_count());
					replacement.decrementIfFIFO(frame);
				}
				// System.out.println("--Buf ID " + frame + " = "
				// + bufDescr[frame].toString() + " Unpin");
			}
			try {
				Page page = new Page();
				SystemDefs.JavabaseDB.read_page(pgid, page);
				int data = Convert.getIntValue(0, page.getpage());
				System.out.println("--Data in " + pgid.pid + " = "
						+ (data - pgid.pid) + " Unpin");
			} catch (InvalidPageNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileIOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 * @throws InvalidRunSizeException
	 * @throws OutOfSpaceException
	 */
	public PageId newPage(Page firstPage, int howmany)
			throws OutOfSpaceException, InvalidRunSizeException,
			InvalidPageNumberException, FileIOException, DiskMgrException,
			IOException {

		if (replacement.getUnpinnedBuffers() == 0) {
			return null;
		} else {
			PageId pageId = new PageId();
			SystemDefs.JavabaseDB.allocate_page(pageId, howmany);
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
	 * @throws IOException
	 * @throws DiskMgrException
	 * @throws FileIOException
	 * @throws InvalidPageNumberException
	 * @throws InvalidRunSizeException
	 */
	public void freePage(PageId pgid) throws InvalidRunSizeException,
			InvalidPageNumberException, FileIOException, DiskMgrException,
			IOException {

		SystemDefs.JavabaseDB.deallocate_page(pgid);
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

			int data = Convert.getIntValue(0, newPage.getpage());
			System.out.println("+++++++ Data in " + pgid.pid + " = "
					+ (data - pgid.pid) + " Frame " + frameNum);
			// if (bufDescr[frameNum].isDirtybit()) {
			SystemDefs.JavabaseDB.write_page(pgid, newPage);
			bufDescr[frameNum].setDirtybit(false);
			// }

		}

	}

	public void flushAllPages() throws InvalidPageNumberException,
			FileIOException, IOException {

		System.out.println("---Flush All Pages---");
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
