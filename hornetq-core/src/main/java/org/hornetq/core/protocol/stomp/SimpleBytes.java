package org.hornetq.core.protocol.stomp;

import java.io.UnsupportedEncodingException;


public class SimpleBytes
{
   private int step;
   private byte[] contents;
   private int index;
   
   public SimpleBytes(int initCapacity)
   {
      this.step = initCapacity;
      contents = new byte[initCapacity];
      index = 0;
   }

   public String getString() throws UnsupportedEncodingException
   {
      if (index == 0) return "";
      byte[] realData = new byte[index];
      System.arraycopy(contents, 0, realData, 0, realData.length);
      
      return new String(realData, "UTF-8");
   }
   
   public void reset()
   {
      index = 0;
   }

   public void append(byte b)
   {
      if (index >= contents.length)
      {
         //grow
         byte[] newBuffer = new byte[contents.length + step];
         System.arraycopy(contents, 0, newBuffer, 0, contents.length);
         contents = newBuffer;
      }
      contents[index++] = b;
   }
}
