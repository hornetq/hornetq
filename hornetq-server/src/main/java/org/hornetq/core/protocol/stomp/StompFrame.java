/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hornetq.core.protocol.stomp;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;

/**
 * Represents all the data in a STOMP frame.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 * @author Tim Fox
 *
 */
public class StompFrame
{
   protected static final byte[] NO_DATA = new byte[] {};

   private static final byte[] END_OF_FRAME = new byte[] { 0, '\n' };

   protected String command;

   protected Map<String, String> headers;

   protected String body;

   protected byte[] bytesBody;

   protected HornetQBuffer buffer = null;

   protected int size;

   protected boolean disconnect;

   protected boolean isPing;

   public StompFrame(String command)
   {
      this(command, false);
   }

   public StompFrame(String command, boolean disconnect)
   {
      this.command = command;
      this.headers = new LinkedHashMap<String, String>();
      this.disconnect = disconnect;
   }

   public StompFrame(String command, Map<String, String> headers,
         byte[] content)
   {
      this.command = command;
      this.headers = headers;
      this.bytesBody = content;
   }

   public String getCommand()
   {
      return command;
   }

   public int getEncodedSize() throws Exception
   {
      if (buffer == null)
      {
         buffer = toHornetQBuffer();
      }
      return size;
   }

   @Override
   public String toString()
   {
      return "StompFrame[command=" + command + ", headers=" + headers + ", content= " + this.body + " bytes " +
               Arrays.toString(bytesBody);
   }

   public boolean isPing()
   {
      return isPing;
   }

   public void setPing(boolean ping)
   {
      isPing = ping;
   }

   public HornetQBuffer toHornetQBuffer() throws Exception
   {
      if (buffer == null)
      {
         if (bytesBody != null)
         {
            buffer = HornetQBuffers.dynamicBuffer(bytesBody.length + 512);
         }
         else
         {
            buffer = HornetQBuffers.dynamicBuffer(512);
         }

         if (isPing())
         {
            buffer.writeByte((byte)10);
            return buffer;
         }

         StringBuffer head = new StringBuffer();
         head.append(command);
         head.append(Stomp.NEWLINE);
         // Output the headers.
         for (Map.Entry<String, String> header : headers.entrySet())
         {
            head.append(header.getKey());
            head.append(Stomp.Headers.SEPARATOR);
            head.append(header.getValue());
            head.append(Stomp.NEWLINE);
         }
         // Add a newline to separate the headers from the content.
         head.append(Stomp.NEWLINE);

         buffer.writeBytes(head.toString().getBytes("UTF-8"));
         if (bytesBody != null)
         {
            buffer.writeBytes(bytesBody);
         }
         buffer.writeBytes(END_OF_FRAME);

         size = buffer.writerIndex();
      }
      return buffer;
   }

   public String getHeader(String key)
   {
      return headers.get(key);
   }

   public void addHeader(String key, String val)
   {
      headers.put(key, val);
   }

   public Map<String, String> getHeadersMap()
   {
      return headers;
   }

   public static class Header
   {
      public String key;
      public String val;

      public Header(String key, String val)
      {
         this.key = key;
         this.val = val;
      }

      public String getEscapedKey()
      {
         return escape(key);
      }

      public String getEscapedValue()
      {
         return escape(val);
      }

      public static String escape(String str)
      {
         int len = str.length();

         char[] buffer = new char[2*len];
         int iBuffer = 0;
         for (int i = 0; i < len; i++)
         {
            char c = str.charAt(i);
            if (c == '\n')
            {
               buffer[iBuffer++] = '\\';
               buffer[iBuffer] = 'n';
            }
            else if (c == '\\')
            {
               buffer[iBuffer++] = '\\';
               buffer[iBuffer] = '\\';
            }
            else if (c == ':')
            {
               buffer[iBuffer++] = '\\';
               buffer[iBuffer] = ':';
            }
            else
            {
               buffer[iBuffer] = c;
            }
            iBuffer++;
         }

         char[] total = new char[iBuffer];
         System.arraycopy(buffer, 0, total, 0, iBuffer);

         return new String(total);
      }
   }

   public void setBody(String body) throws UnsupportedEncodingException
   {
      this.body = body;
      this.bytesBody = body.getBytes("UTF-8");
   }

   public boolean hasHeader(String key)
   {
      return headers.containsKey(key);
   }

   public String getBody() throws UnsupportedEncodingException
   {
      if (body == null)
      {
         if (bytesBody != null)
         {
            body = new String(bytesBody, "UTF-8");
         }
      }
      return body;
   }

   //Since 1.1, there is a content-type header that needs to take care of
   public byte[] getBodyAsBytes() throws UnsupportedEncodingException
   {
      return bytesBody;
   }

   public boolean needsDisconnect()
   {
      return disconnect;
   }

   public void setByteBody(byte[] content)
   {
      this.bytesBody = content;
   }

   public void setNeedsDisconnect(boolean b)
   {
      disconnect = b;
   }
}
