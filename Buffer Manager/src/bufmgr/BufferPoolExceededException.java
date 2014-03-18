package bufmgr;

import chainexception.*;

@SuppressWarnings("serial")
public class BufferPoolExceededException extends ChainException {
	public BufferPoolExceededException(Exception ex, String name) {
		super(ex, name);
	}
}
