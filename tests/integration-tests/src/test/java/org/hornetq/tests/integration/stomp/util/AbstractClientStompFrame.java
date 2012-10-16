/*
 * Copyright 2010 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.integration.stomp.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public abstract class AbstractClientStompFrame implements ClientStompFrame
{
   protected static final String HEADER_RECEIPT = "receipt";

   protected String command;
   protected List<Header> headers = new ArrayList<Header>();
   protected Set<String> headerKeys = new HashSet<String>();
   protected String body;

   public AbstractClientStompFrame(String command)
   {
      this.command = command;
   }

   public String toString()
   {
      StringBuffer sb = new StringBuffer("Frame: <" + command + ">" + "\n");
      Iterator<Header> iter = headers.iterator();
      while (iter.hasNext())
      {
         Header h = iter.next();
         sb.append(h.key + ":" + h.val + "\n");
      }
      sb.append("\n");
      sb.append("<body>" + body + "<body>");
      return sb.toString();
   }

   @Override
   public ByteBuffer toByteBuffer() throws UnsupportedEncodingException
   {
      if (isPing())
      {
         ByteBuffer buffer = ByteBuffer.allocateDirect(1);
         buffer.put((byte)0x0A);
         buffer.rewind();
         return buffer;
      }
      StringBuffer sb = new StringBuffer();
      sb.append(command + "\n");
      int n = headers.size();
      for (int i = 0; i < n; i++)
      {
         sb.append(headers.get(i).key + ":" + headers.get(i).val + "\n");
      }
      sb.append("\n");
      if (body != null)
      {
         sb.append(body);
      }
      sb.append((char)0);

      String data = sb.toString();

      byte[] byteValue = data.getBytes("UTF-8");

      ByteBuffer buffer = ByteBuffer.allocateDirect(byteValue.length);
      buffer.put(byteValue);

      buffer.rewind();
      return buffer;
   }

   @Override
   public ByteBuffer toByteBufferWithExtra(String str) throws UnsupportedEncodingException
   {
      StringBuffer sb = new StringBuffer();
      sb.append(command + "\n");
      int n = headers.size();
      for (int i = 0; i < n; i++)
      {
         sb.append(headers.get(i).key + ":" + headers.get(i).val + "\n");
      }
      sb.append("\n");
      if (body != null)
      {
         sb.append(body);
      }
      sb.append((char)0);
      sb.append(str);

      String data = sb.toString();

      byte[] byteValue = data.getBytes("UTF-8");

      ByteBuffer buffer = ByteBuffer.allocateDirect(byteValue.length);
      buffer.put(byteValue);

      buffer.rewind();
      return buffer;
   }

   @Override
   public boolean needsReply()
   {
      if ("CONNECT".equals(command) || headerKeys.contains(HEADER_RECEIPT))
      {
         return true;
      }
      return false;
   }

   @Override
   public void setCommand(String command)
   {
      this.command = command;
   }

   @Override
   public void addHeader(String key, String val)
   {
      headers.add(new Header(key, val));
      headerKeys.add(key);
   }

   @Override
   public void setBody(String body)
   {
      this.body = body;
   }

   @Override
   public String getBody()
   {
      return body;
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

   @Override
   public String getCommand()
   {
      return command;
   }

   @Override
   public String getHeader(String header)
   {
      if (headerKeys.contains(header))
      {
         Iterator<Header> iter = headers.iterator();
         while (iter.hasNext())
         {
            Header h = iter.next();
            if (h.key.equals(header))
            {
               return h.val;
            }
         }
      }
      return null;
   }

}
