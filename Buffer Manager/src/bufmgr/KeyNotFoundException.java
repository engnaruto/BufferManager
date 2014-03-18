package bufmgr;

import chainexception.*;

@SuppressWarnings("serial")
public class KeyNotFoundException extends ChainException {

	public KeyNotFoundException(Exception e, String name) {
		super(e, name);
	}

}
