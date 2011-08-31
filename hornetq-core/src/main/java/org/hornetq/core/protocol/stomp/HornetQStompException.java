package org.hornetq.core.protocol.stomp;

import java.util.ArrayList;
import java.util.List;

public class HornetQStompException extends Exception {

   private static final long serialVersionUID = -274452327574950068L;
   
   private List<Header> headers = new ArrayList<Header>(10);
   private String body;
   
   public HornetQStompException(String msg)
   {
      super(msg);
   }
   
   public HornetQStompException(String msg, Throwable t)
   {
      super(msg, t);
   }
   
   public HornetQStompException(Throwable t)
   {
      super(t);
   }

   public void addHeader(String header, String value)
   {
      headers.add(new Header(header, value));
   }
   
   public void setBody(String body)
   {
      this.body = body;
   }

   private class Header
   {
      public String key;
      public String val;
      
      public Header(String key, String val)
      {
         this.key = key;
         this.val = val;
      }
   }
}
