package diskmgr;
import chainexception.*;


@SuppressWarnings("serial")
public class FileIOException extends ChainException {

  public FileIOException(Exception e, String name)
  
  { 
    super(e, name); 
  }


}




