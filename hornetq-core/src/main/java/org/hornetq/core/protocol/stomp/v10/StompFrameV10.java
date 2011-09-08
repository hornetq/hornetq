package org.hornetq.core.protocol.stomp.v10;

import org.hornetq.core.protocol.stomp.HornetQStompException;
import org.hornetq.core.protocol.stomp.StompFrame;

public class StompFrameV10 extends StompFrame
{
   public StompFrameV10(String command)
   {
      super(command);
   }
   
   @Override
   public void addHeader(String key, String val) throws HornetQStompException
   {
      //trimming
      String newKey = key.trim();
      String newVal = val.trim();
      super.addHeader(newKey, newVal);
   }

}
