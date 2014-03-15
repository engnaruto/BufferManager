package bufmgr;

import chainexception.*;

@SuppressWarnings("serial")
public class PageIdNotInTheBufferPoolExcpetion extends ChainException {

	public PageIdNotInTheBufferPoolExcpetion(Exception e, String name) {
		super(e, name);
	}

}
