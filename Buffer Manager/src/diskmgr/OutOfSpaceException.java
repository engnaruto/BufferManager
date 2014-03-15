package diskmgr;
import chainexception.*;

@SuppressWarnings("serial")
public class OutOfSpaceException extends ChainException {

  public OutOfSpaceException(Exception e, String name)
    { 
      super(e, name); 
    }
}

