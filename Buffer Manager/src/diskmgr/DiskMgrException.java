package diskmgr;
import chainexception.*;


@SuppressWarnings("serial")
public class DiskMgrException extends ChainException {

  public DiskMgrException(Exception e, String name)
  
  { 
    super(e, name); 
  }


}




