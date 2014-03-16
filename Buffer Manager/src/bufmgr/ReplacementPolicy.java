package bufmgr;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

public class ReplacementPolicy {
	private int unpinnedBufs;
	private String replaceArg;
	private int[] fifo;
	private Queue<Integer> lru;
	private Stack<Integer> mru;

	// Love/Hate ?????

	public ReplacementPolicy(int numBufs, String replaceArg) {
		System.out.println("numBufs = " + numBufs + " replaceArg = "
				+ replaceArg);
		unpinnedBufs = numBufs;
		this.replaceArg = replaceArg;
		if (replaceArg.equals("FIFO") || replaceArg.equals("Clock")) {
			fifo = new int[numBufs];
			Arrays.fill(fifo, -1);
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

	public void incrementIfFIFO(int frame) {
		if (replaceArg.equals("FIFO")) {
			if (fifo[frame] == -1) {
				fifo[frame] = 1;
			} else {
				fifo[frame]++;
			}
		}
//		System.out.println(fifo[frame]);
	}

	public void decrementIfFIFO(int frame) {
		if (replaceArg.equals("FIFO")) {
			if (fifo[frame] > 0) {
				fifo[frame]--;
			}
		}
	}

	public int getFreeFrame() {
		if (replaceArg.equals("FIFO")) {
			unpinnedBufs--;
			return getFIFO();
		} else if (replaceArg.equals("LRU")) {
			unpinnedBufs--;
			return getLRU();
		} else if (replaceArg.equals("MRU")) {
			unpinnedBufs--;
			return getMRU();
		} else {
			unpinnedBufs--;
			return getLoveHate();
		}
	}

	public void returnFrame(int frameNum) {
		if (replaceArg.equals("FIFO")) {
			unpinnedBufs++;
			returnFIFO(frameNum);
		} else if (replaceArg.equals("LRU")) {
			unpinnedBufs++;
			returnLRU(frameNum);
		} else if (replaceArg.equals("MRU")) {
			unpinnedBufs++;
			returnMRU(frameNum);
		} else {
			unpinnedBufs++;
			returnLoveHate(frameNum);
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
		unpinnedBufs--;
	}

	// --------------Get-----------------

	private int getFIFO() {

		for (int i = 0; i < fifo.length; i++) {
			// System.out.println(Arrays.toString(fifo));
			if (fifo[i] == -1) {
				return i;
			}
		}
		for (int i = 0; i < fifo.length; i++) {
			if (fifo[i] == 0) {
				return i;
			}
		}
		try {
			throw new FullBufferPoolExcpetion(null, "FullBufferPoolExcpetion");
		} catch (FullBufferPoolExcpetion e) {
			e.printStackTrace();
		}
		return -1;
	}

	private int getLRU() {
		if (!lru.isEmpty()) {
			return lru.poll();
		} else {
			try {
				throw new FullBufferPoolExcpetion(null,
						"FullBufferPoolExcpetion");
			} catch (FullBufferPoolExcpetion e) {
				e.printStackTrace();
			}
			return -1;
		}

	}

	private int getMRU() {
		if (!mru.isEmpty()) {
			return lru.poll();
		} else {
			try {
				throw new FullBufferPoolExcpetion(null,
						"FullBufferPoolExcpetion");
			} catch (FullBufferPoolExcpetion e) {
				e.printStackTrace();
			}
			return -1;
		}
	}

	private int getLoveHate() {
		return 0;
	}

	// --------------Remove-----------------
	private void removeFIFO(int frameNum) {
		fifo[frameNum]++;
//		System.out.println(fifo[frameNum]);
	}

	private void removeMRU(int frameNum) {
		mru.remove(new Integer(frameNum));
	}

	private void removeLRU(int frameNum) {
		lru.remove(new Integer(frameNum));
	}

	private void removeLoveHate(int frameNum) {

	}

	// --------------Return-----------------

	private void returnFIFO(int frameNum) {
		fifo[frameNum] = 0;
	}

	private void returnMRU(int frameNum) {
		mru.add(frameNum);
	}

	private void returnLRU(int frameNum) {
		lru.add(frameNum);
	}

	private void returnLoveHate(int frameNum) {

	}

}
