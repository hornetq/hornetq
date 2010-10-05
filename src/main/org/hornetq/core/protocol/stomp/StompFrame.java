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

import java.util.Map;
import java.util.Map.Entry;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.logging.Logger;

/**
 * Represents all the data in a STOMP frame.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 * @author Tim Fox
 * 
 */
class StompFrame
{
   private static final Logger log = Logger.getLogger(StompFrame.class);

   public static final byte[] NO_DATA = new byte[] {};

   private static final byte[] END_OF_FRAME = new byte[] { 0, '\n' };

   private final String command;

   private final Map<String, Object> headers;

   private final byte[] content;

   private HornetQBuffer buffer = null;

   private int size;
   
   public StompFrame(String command, Map<String, Object> headers, byte[] data)
   {
      this.command = command;
      this.headers = headers;
      this.content = data;
   }

   public StompFrame(String command, Map<String, Object> headers)
   {
      this.command = command;
      this.headers = headers;
      this.content = NO_DATA;
   }

   public String getCommand()
   {
      return command;
   }

   public byte[] getContent()
   {
      return content;
   }
   
   public Map<String, Object> getHeaders()
   {
      return headers;
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
      return "StompFrame[command=" + command + ", headers=" + headers + ", content-length=" + content.length + "]";
   }

   public String asString()
   {
      String out = command + '\n';
      for (Entry<String, Object> header : headers.entrySet())
      {
         out += header.getKey() + ": " + header.getValue() + '\n';
      }
      out += '\n';
      out += new String(content);
      return out;
   }

 
   public HornetQBuffer toHornetQBuffer() throws Exception
   {
      if (buffer == null)
      {
         buffer = HornetQBuffers.dynamicBuffer(content.length + 512);

         StringBuffer head = new StringBuffer();
         head.append(command);
         head.append(Stomp.NEWLINE);
         // Output the headers.
         for (Map.Entry<String, Object> header : headers.entrySet())
         {
            head.append(header.getKey());
            head.append(Stomp.Headers.SEPARATOR);
            head.append(header.getValue());
            head.append(Stomp.NEWLINE);
         }
         // Add a newline to separate the headers from the content.
         head.append(Stomp.NEWLINE);

         buffer.writeBytes(head.toString().getBytes("UTF-8"));
         buffer.writeBytes(content);
         buffer.writeBytes(END_OF_FRAME);

         size = buffer.writerIndex();
      }
      return buffer;
   }
}
