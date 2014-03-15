package diskmgr;
import chainexception.*;

@SuppressWarnings("serial")
public class InvalidPageNumberException extends ChainException {
  
  
  public InvalidPageNumberException(Exception ex, String name) 
    { 
      super(ex, name); 
    }
}




