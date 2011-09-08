package org.hornetq.core.protocol.stomp.v11;

import java.util.Map;

import org.hornetq.core.protocol.stomp.HornetQStompException;
import org.hornetq.core.protocol.stomp.StompFrame;

public class StompFrameV11 extends StompFrame
{
   public static final char ESC_CHAR = '\\';
   public static final char COLON = ':';

   public StompFrameV11(String command, Map<String, String> headers, byte[] content)
   {
      super(command, headers, content);
   }
   
   public StompFrameV11(String command)
   {
      super(command);
   }

   public static String escaping(String rawString) throws HornetQStompException
   {
      int len = rawString.length();

      SimpleBytes sb = new SimpleBytes(1024);
      
      boolean beginEsc = false;
      for (int i = 0; i < len; i++)
      {
         char k = rawString.charAt(i);

         if (k == ESC_CHAR)
         {
            if (beginEsc)
            {
               //it is a backslash
               sb.append('\\');
               beginEsc = false;
            }
            else
            {
               beginEsc = true;
            }
         }
         else if (k == 'n')
         {
            if (beginEsc)
            {
               //it is a newline
               sb.append('\n');
               beginEsc = false;
            }
            else
            {
               sb.append(k);
            }
         }
         else if (k == ':')
         {
            if (beginEsc)
            {
               sb.append(k);
               beginEsc = false;
            }
            else
            {
               //error
               throw new HornetQStompException("Colon not escaped!");
            }
         }
         else
         {
            if (beginEsc)
            {
               //error, no other escape defined.
               throw new HornetQStompException("Bad escape char found: " + k);
            }
            else
            {
               sb.append(k);
            }
         }
      }
      return sb.toString();
   }

   public static void main(String[] args)
   {
      String rawStr = "hello world\\n\\:"
   }

}
