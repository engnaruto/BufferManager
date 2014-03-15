package bufmgr;
import chainexception.*;



@SuppressWarnings("serial")
public class PageUnpinnedExcpetion extends ChainException {
	public PageUnpinnedExcpetion(Exception ex, String name) {
		super(ex, name);
	}
}
