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

import java.io.IOException;
import java.util.HashMap;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.logging.Logger;

/**
 * Implements marshalling and unmarsalling the <a href="http://stomp.codehaus.org/">Stomp</a> protocol.
 */
class StompFrameDecoder
{
   private static final Logger log = Logger.getLogger(StompFrameDecoder.class);

   private static final int MAX_COMMAND_LENGTH = 1024;

   private static final int MAX_HEADER_LENGTH = 1024 * 10;

   private static final int MAX_HEADERS = 1000;

   private static final int MAX_DATA_LENGTH = 1024 * 1024 * 10;

   public StompFrame decode(HornetQBuffer buffer)
   {
      try
      {
         String command = null;

         // skip white space to next real action line
         while (true) {
            command = StompFrameDecoder.readLine(buffer, StompFrameDecoder.MAX_COMMAND_LENGTH, "The maximum command length was exceeded");
             if (command == null) {
                return null;
             }
             else {
                command = command.trim();
                 if (command.length() > 0) {
                     break;
                 }
             }
         }
         
         // Parse the headers
         HashMap<String, Object> headers = new HashMap<String, Object>(25);
         while (true)
         {
            String line = StompFrameDecoder.readLine(buffer, StompFrameDecoder.MAX_HEADER_LENGTH, "The maximum header length was exceeded");
            if (line == null)
            {
               return null;
            }

            if (headers.size() > StompFrameDecoder.MAX_HEADERS)
            {
               throw new StompException("The maximum number of headers was exceeded", true);
            }

            if (line.trim().length() == 0)
            {
               break;
            }

            try
            {
               int seperator_index = line.indexOf(Stomp.Headers.SEPARATOR);
               if (seperator_index == -1)
               {
                  return null;
               }
               String name = line.substring(0, seperator_index).trim();
               String value = line.substring(seperator_index + 1, line.length()).trim();
               headers.put(name, value);
            }
            catch (Exception e)
            {
               throw new StompException("Unable to parse header line [" + line + "]", true);
            }
         }
         // Read in the data part.
         byte[] data = StompFrame.NO_DATA;
         String contentLength = (String)headers.get(Stomp.Headers.CONTENT_LENGTH);
         if (contentLength != null)
         {

            // Bless the client, he's telling us how much data to read in.
            int length;
            try
            {
               length = Integer.parseInt(contentLength.trim());
            }
            catch (NumberFormatException e)
            {
               throw new StompException("Specified content-length is not a valid integer", true);
            }

            if (length > StompFrameDecoder.MAX_DATA_LENGTH)
            {
               throw new StompException("The maximum data length was exceeded", true);
            }

            if (buffer.readableBytes() < length)
            {
               return null;
            }
            
            data = new byte[length];
            buffer.readBytes(data);

            if (!buffer.readable())
            {
               return null;
            }
            if (buffer.readByte() != 0)
            {
               throw new StompException(Stomp.Headers.CONTENT_LENGTH + " bytes were read and " +
                                        "there was no trailing null byte", true);
            }
         }
         else
         {
            byte[] body = new byte[StompFrameDecoder.MAX_DATA_LENGTH];
            boolean bodyCorrectlyEnded = false;
            int count = 0;
            while (buffer.readable())
            {
               byte b = buffer.readByte();

               if (b == (byte)'\0')
               {
                  bodyCorrectlyEnded = true;
                  break;
               }
               else
               {
                  body[count++] = b;
               }
            }

            if (!bodyCorrectlyEnded)
            {
               return null;
            }

            data = new byte[count];
            System.arraycopy(body, 0, data, 0, count);
         }

         return new StompFrame(command, headers, data);
      }
      catch (IOException e)
      {
         log.error("Unable to decode stomp frame", e);
         return null;
      }
   }
   
   private static String readLine(HornetQBuffer in, int maxLength, String errorMessage) throws IOException
   {
      char[] chars = new char[MAX_HEADER_LENGTH];

      if (!in.readable())
      {
         return null;
      }
      
      boolean properString = false;
      int count = 0;
      while (in.readable())
      {
         byte b = in.readByte();

         if (b == (byte)'\n')
         {
            properString = true;
            break;
         }
         else
         {
            chars[count++] = (char)b;
         }
      }
      if (properString)
      {
         return new String(chars, 0, count);
      }
      else
      {
         return null;
      }
   }
}
