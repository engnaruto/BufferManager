package bufmgr;

import chainexception.*;

@SuppressWarnings("serial")
public class PagePinnedException extends ChainException {
	public PagePinnedException(Exception ex, String name) {
		super(ex, name);
	}
}
