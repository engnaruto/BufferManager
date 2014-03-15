package bufmgr;

import chainexception.*;

@SuppressWarnings("serial")
public class WrongArgumentExcpetion extends ChainException {

	public WrongArgumentExcpetion(Exception e, String name) {
		super(e, name);
	}

}
