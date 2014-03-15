package bufmgr;

import chainexception.*;

@SuppressWarnings("serial")
public class FullBufferPoolExcpetion extends ChainException {
	public FullBufferPoolExcpetion(Exception ex, String name) {
		super(ex, name);
	}
}
