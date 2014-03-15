package diskmgr;
import chainexception.*;


@SuppressWarnings("serial")
public class FileEntryNotFoundException extends ChainException {

  public FileEntryNotFoundException(Exception e, String name)
  { 
    super(e, name); 
  }

  


}




