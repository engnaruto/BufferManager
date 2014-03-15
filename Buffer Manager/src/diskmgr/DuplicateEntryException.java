package diskmgr;
import chainexception.*;

@SuppressWarnings("serial")
public class DuplicateEntryException extends ChainException {
  
  public DuplicateEntryException(Exception e, String name)
    {
      super(e, name); 
    }
}

