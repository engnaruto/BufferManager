package diskmgr;
import chainexception.*;

@SuppressWarnings("serial")
public class InvalidRunSizeException extends ChainException {
  
  public InvalidRunSizeException(Exception e, String name)
    { 
      super(e, name); 
    }
}




