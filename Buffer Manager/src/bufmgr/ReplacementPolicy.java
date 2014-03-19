package bufmgr;

import java.util.*;

public class ReplacementPolicy {
	private int unpinnedBufs;
	private int bufferSize;
	private int counter;
	private String replaceArg;
	private PriorityQueue<Integer> fifo;

	private Queue<Integer> lru;
	private Stack<Integer> mru;

	// Love/Hate ?????

	public ReplacementPolicy(int numBufs, String replaceArg) {
		System.out.println("numBufs = " + numBufs + " replaceArg = "
				+ replaceArg);
		bufferSize = numBufs;
		unpinnedBufs = numBufs;
		counter = 0;
		this.replaceArg = replaceArg;
		if (replaceArg.equals("FIFO") || replaceArg.equals("Clock")) {
			fifo = new PriorityQueue<Integer>();
			this.replaceArg = "FIFO";
		} else if (replaceArg.equals("LRU")) {
			lru = new LinkedList<Integer>();
			for (int i = 0; i < numBufs; i++) {
				lru.add(i);
			}
		} else if (replaceArg.equals("MRU")) {
			mru = new Stack<Integer>();
			for (int i = 0; i < numBufs; i++) {
				mru.add(i);
			}
		} else if (replaceArg.equals("Love/Hate")) {

		} else {
			try {
				throw new WrongArgumentExcpetion(null, "WrongArgumentExcpetion");
			} catch (WrongArgumentExcpetion e) {
				e.printStackTrace();
			}
		}
	}

	public int getUnpinnedBuffers() {
		return unpinnedBufs;
	}

	public int getFreeFrame() throws BufferPoolExceededException {
		if (unpinnedBufs <= 0) {
			// throw new BufferPoolExceededException(null,
			// "BufferPoolExceededException");
			throw new BufferPoolExceededException(null,
					"BufferPoolExceededException");

		} else {
			// System.out.println("Counter");
			if (counter != bufferSize) {
				unpinnedBufs--;
				return counter++;
			} else {
				if (replaceArg.equals("FIFO")) {
					return getFIFO();
				} else if (replaceArg.equals("LRU")) {
					return getLRU();
				} else if (replaceArg.equals("MRU")) {
					return getMRU();
				} else {
					return getLoveHate();
				}
			}
		}
	}

	public void returnFrame(int frameNum) {
		if (replaceArg.equals("FIFO")) {
			returnFIFO(frameNum);
		} else if (replaceArg.equals("LRU")) {
			returnLRU(frameNum);
		} else if (replaceArg.equals("MRU")) {
			returnMRU(frameNum);
		} else {
			returnLoveHate(frameNum);
		}
		unpinnedBufs++;
		if (unpinnedBufs > bufferSize) {
			unpinnedBufs = bufferSize;
		}
	}

	public void removeFrame(int frameNum) {
		if (replaceArg.equals("FIFO")) {
			removeFIFO(frameNum);
		} else if (replaceArg.equals("LRU")) {
			removeLRU(frameNum);
		} else if (replaceArg.equals("MRU")) {
			removeMRU(frameNum);
		} else {
			removeLoveHate(frameNum);
		}
		if (unpinnedBufs > bufferSize) {
			unpinnedBufs = bufferSize;
		}
	}

	// --------------Get-----------------

	private int getFIFO() throws BufferPoolExceededException {
		if (!fifo.isEmpty()) {
			unpinnedBufs--;
			// Object[] a = new int[0] ;
			// a=fifo.toArray();
			// Arrays.sort(a);
			// System.out.println("++FIFO = " + a.toString());
			return fifo.poll();
		} else {
			throw new BufferPoolExceededException(null,
					"BufferPoolExceededException");
		}
	}

	private int getLRU() throws BufferPoolExceededException {
		if (!lru.isEmpty()) {
			unpinnedBufs--;
			return lru.poll();
		} else {
			throw new BufferPoolExceededException(null,
					"BufferPoolExceededException");
		}

	}

	private int getMRU() throws BufferPoolExceededException {
		if (!mru.isEmpty()) {
			unpinnedBufs--;
			return lru.poll();
		} else {
			throw new BufferPoolExceededException(null,
					"BufferPoolExceededException");
		}
	}

	private int getLoveHate() {
		return 0;
	}

	// --------------Remove-----------------
	private void removeFIFO(int frameNum) {
		if (fifo.contains(frameNum)) {
			unpinnedBufs--;
			fifo.remove(new Integer(frameNum));
		}
	}

	private void removeMRU(int frameNum) {
		if (mru.contains(frameNum)) {
			unpinnedBufs--;
			mru.remove(new Integer(frameNum));
		}
	}

	private void removeLRU(int frameNum) {
		if (lru.contains(frameNum)) {
			unpinnedBufs--;
			lru.remove(new Integer(frameNum));
		}
	}

	private void removeLoveHate(int frameNum) {

	}

	// --------------Return-----------------

	private void returnFIFO(int frameNum) {
		if (!fifo.contains(frameNum)) {
			fifo.add(frameNum);
		}
	}

	private void returnMRU(int frameNum) {
		if (!mru.contains(frameNum)) {
			mru.add(frameNum);
		}
	}

	private void returnLRU(int frameNum) {
		if (!lru.contains(frameNum)) {
			lru.add(frameNum);
		}
	}

	private void returnLoveHate(int frameNum) {

	}

	// -----------------------------------------------

}
