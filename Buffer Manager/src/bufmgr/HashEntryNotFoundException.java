package bufmgr;

import chainexception.*;

@SuppressWarnings("serial")
public class HashEntryNotFoundException extends ChainException {

	public HashEntryNotFoundException(Exception e, String name) {
		super(e, name);
	}

}
