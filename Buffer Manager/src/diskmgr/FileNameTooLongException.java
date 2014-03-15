package diskmgr;
import chainexception.*;


@SuppressWarnings("serial")
public class FileNameTooLongException extends ChainException {
  
  public FileNameTooLongException(Exception ex, String name)
    { 
      super(ex, name); 
    }
  
}




