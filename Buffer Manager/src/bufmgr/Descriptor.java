package bufmgr;

import global.PageId;

public class Descriptor {
	protected int pin_count;
	protected PageId pagenumber;
	protected boolean dirtybit;

	public Descriptor() {
		pin_count = 0;
		pagenumber = null;
		dirtybit = false;
	}

	public Descriptor(PageId pageId, int pin) {
		pin_count = pin;
		pagenumber = pageId;
		dirtybit = false;
	}

	public int getPin_count() {
		return pin_count;
	}

	public void setPin_count(int pin_count) {
		this.pin_count = pin_count;
	}

	public PageId getPagenumber() {
		return pagenumber;
	}

	public void setPagenumber(PageId pagenumber) {
		this.pagenumber = pagenumber;
	}

	public boolean isDirtybit() {
		return dirtybit;
	}

	public void setDirtybit(boolean dirtybit) {
		this.dirtybit = dirtybit;
	}

	@Override
	public String toString() {
		return pagenumber + " " + pin_count + " " + dirtybit;
	}

}
