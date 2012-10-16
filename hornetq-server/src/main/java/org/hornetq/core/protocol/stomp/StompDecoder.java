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

package org.hornetq.core.protocol.stomp;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.HornetQBuffer;

/**
 * A StompDecoder
 *
 * @author Tim Fox
 *
 *
 */
public class StompDecoder
{
   public static final boolean TRIM_LEADING_HEADER_VALUE_WHITESPACE = true;

   public static final String COMMAND_ABORT = "ABORT";

   public static final int COMMAND_ABORT_LENGTH = COMMAND_ABORT.length();

   public static final String COMMAND_ACK = "ACK";

   public static final int COMMAND_ACK_LENGTH = COMMAND_ACK.length();

   public static final String COMMAND_NACK = "NACK";

   public static final int COMMAND_NACK_LENGTH = COMMAND_NACK.length();

   public static final String COMMAND_BEGIN = "BEGIN";

   public static final int COMMAND_BEGIN_LENGTH = COMMAND_BEGIN.length();

   public static final String COMMAND_COMMIT = "COMMIT";

   public static final int COMMAND_COMMIT_LENGTH = COMMAND_COMMIT.length();

   public static final String COMMAND_CONNECT = "CONNECT";

   public static final int COMMAND_CONNECT_LENGTH = COMMAND_CONNECT.length();

   public static final String COMMAND_DISCONNECT = "DISCONNECT";

   public static final int COMMAND_DISCONNECT_LENGTH = COMMAND_DISCONNECT.length();

   public static final String COMMAND_SEND = "SEND";

   public static final int COMMAND_SEND_LENGTH = COMMAND_SEND.length();

   public static final String COMMAND_STOMP = "STOMP";

   public static final int COMMAND_STOMP_LENGTH = COMMAND_STOMP.length();

   public static final String COMMAND_SUBSCRIBE = "SUBSCRIBE";

   public static final int COMMAND_SUBSCRIBE_LENGTH = COMMAND_SUBSCRIBE.length();

   public static final String COMMAND_UNSUBSCRIBE = "UNSUBSCRIBE";

   public static final int COMMAND_UNSUBSCRIBE_LENGTH = COMMAND_UNSUBSCRIBE.length();

   /**** added by meddy, 27 april 2011, handle header parser for reply to websocket protocol ****/
   public static final String COMMAND_CONNECTED = "CONNECTED";

   public static final int COMMAND_CONNECTED_LENGTH = COMMAND_CONNECTED.length();

   public static final String COMMAND_MESSAGE = "MESSAGE";

   public static final int COMMAND_MESSAGE_LENGTH = COMMAND_MESSAGE.length();

   public static final String COMMAND_ERROR = "ERROR";

   public static final int COMMAND_ERROR_LENGTH = COMMAND_ERROR.length();

   public static final String COMMAND_RECEIPT = "RECEIPT";

   public static final int COMMAND_RECEIPT_LENGTH = COMMAND_RECEIPT.length();
   /**** end  ****/

   public static final byte A = (byte)'A';

   public static final byte B = (byte)'B';

   public static final byte C = (byte)'C';

   public static final byte D = (byte)'D';

   public static final byte E = (byte)'E';

   public static final byte T = (byte)'T';

   public static final byte M = (byte)'M';

   public static final byte S = (byte)'S';

   public static final byte R = (byte)'R';

   public static final byte U = (byte)'U';

   public static final byte N = (byte)'N';

   public static final byte LN = (byte)'n';

   public static final byte HEADER_SEPARATOR = (byte)':';

   public static final byte NEW_LINE = (byte)'\n';

   public static final byte SPACE = (byte)' ';

   public static final byte TAB = (byte)'\t';

   public static final String CONTENT_TYPE_HEADER_NAME = "content-type";

   public static final String CONTENT_LENGTH_HEADER_NAME = "content-length";

   public byte[] workingBuffer = new byte[1024];

   public int pos;

   public int data;

   public String command;

   public Map<String, String> headers;

   public int headerBytesCopyStart;

   public boolean readingHeaders;

   public boolean headerValueWhitespace;

   public boolean inHeaderName;

   public String headerName;

   public boolean whiteSpaceOnly;

   public int contentLength;

   public String contentType;

   public int bodyStart;

   public StompConnection connection;

   public StompDecoder(StompConnection stompConnection)
   {
      this.connection = stompConnection;
      init();
   }

   public StompDecoder()
   {
      init();
   }

   public boolean hasBytes()
   {
      return data > pos;
   }

   /*
    * Stomp format is a command on the first line
    * followed by a set of headers NAME:VALUE
    * followed by an empty line
    * followed by an optional message body
    * terminated with a null character
    *
    * Note: to support both 1.0 and 1.1, we just assemble a
    * standard StompFrame and let the versioned handler to do more
    * spec specific job (like trimming, escaping etc).
    */
   public synchronized StompFrame decode(final HornetQBuffer buffer) throws Exception
   {
      if (connection != null && connection.isValid())
      {
         VersionedStompFrameHandler handler = connection.getFrameHandler();
         return handler.decode(this, buffer);
      }

      return defaultDecode(buffer);
   }

   public StompFrame defaultDecode(final HornetQBuffer buffer) throws HornetQStompException
   {

      int readable = buffer.readableBytes();

      if (data + readable >= workingBuffer.length)
      {
         resizeWorking(data + readable);
      }

      buffer.readBytes(workingBuffer, data, readable);

      data += readable;

      if (command == null)
      {
         if (data < 4)
         {
            // Need at least four bytes to identify the command
            // - up to 3 bytes for the command name + potentially another byte for a leading \n

            return null;
         }

         int offset;

         if (workingBuffer[0] == NEW_LINE)
         {
            // Yuck, some badly behaved STOMP clients add a \n *after* the terminating NUL char at the end of the
            // STOMP
            // frame this can manifest as an extra \n at the beginning when the next STOMP frame is read - we need to
            // deal
            // with this
            offset = 1;
         }
         else
         {
            offset = 0;
         }

         byte b = workingBuffer[offset];

         switch (b)
         {
            case A:
            {
               if (workingBuffer[offset + 1] == B)
               {
                  if (!tryIncrement(offset + COMMAND_ABORT_LENGTH + 1))
                  {
                     return null;
                  }

                  // ABORT
                  command = COMMAND_ABORT;
               }
               else
               {
                  if (!tryIncrement(offset + COMMAND_ACK_LENGTH + 1))
                  {
                     return null;
                  }

                  // ACK
                  command = COMMAND_ACK;
               }
               break;
            }
            case B:
            {
               if (!tryIncrement(offset + COMMAND_BEGIN_LENGTH + 1))
               {
                  return null;
               }

               // BEGIN
               command = COMMAND_BEGIN;

               break;
            }
            case C:
            {
               if (workingBuffer[offset + 2] == M)
               {
                  if (!tryIncrement(offset + COMMAND_COMMIT_LENGTH + 1))
                  {
                     return null;
                  }

                  // COMMIT
                  command = COMMAND_COMMIT;
               }
               /**** added by meddy, 27 april 2011, handle header parser for reply to websocket protocol ****/
               else if (workingBuffer[offset+7]==E)
               {
                  if (!tryIncrement(offset + COMMAND_CONNECTED_LENGTH + 1))
                  {
                     return null;
                  }

                  // CONNECTED
                  command = COMMAND_CONNECTED;
               }
               /**** end ****/
               else
               {
                  if (!tryIncrement(offset + COMMAND_CONNECT_LENGTH + 1))
                  {
                     return null;
                  }

                  // CONNECT
                  command = COMMAND_CONNECT;
               }
               break;
            }
            case D:
            {
               if (!tryIncrement(offset + COMMAND_DISCONNECT_LENGTH + 1))
               {
                  return null;
               }

               // DISCONNECT
               command = COMMAND_DISCONNECT;

               break;
            }
            case R:
            {
               if (!tryIncrement(offset + COMMAND_RECEIPT_LENGTH + 1))
               {
                  return null;
               }

               // RECEIPT
               command = COMMAND_RECEIPT;

               break;
            }
            /**** added by meddy, 27 april 2011, handle header parser for reply to websocket protocol ****/
            case E:
            {
               if (!tryIncrement(offset + COMMAND_ERROR_LENGTH + 1))
               {
                  return null;
               }

               // ERROR
               command = COMMAND_ERROR;

               break;
            }
            case M:
            {
               if (!tryIncrement(offset + COMMAND_MESSAGE_LENGTH + 1))
               {
                  return null;
               }

               // MESSAGE
               command = COMMAND_MESSAGE;

               break;
            }
            /**** end ****/
            case S:
            {
               if (workingBuffer[offset + 1] == E)
               {
                  if (!tryIncrement(offset + COMMAND_SEND_LENGTH + 1))
                  {
                     return null;
                  }

                  // SEND
                  command = COMMAND_SEND;
               }
               else if (workingBuffer[offset + 1] == T)
               {
                  if (!tryIncrement(offset + COMMAND_STOMP_LENGTH + 1))
                  {
                     return null;
                  }

                  // SEND
                  command = COMMAND_STOMP;
               }
               else
               {
                  if (!tryIncrement(offset + COMMAND_SUBSCRIBE_LENGTH + 1))
                  {
                     return null;
                  }

                  // SUBSCRIBE
                  command = COMMAND_SUBSCRIBE;
               }
               break;
            }
            case U:
            {
               if (!tryIncrement(offset + COMMAND_UNSUBSCRIBE_LENGTH + 1))
               {
                  return null;
               }

               // UNSUBSCRIBE
               command = COMMAND_UNSUBSCRIBE;

               break;
            }
            default:
            {
               throwInvalid();
            }
         }

         // Sanity check

         if (workingBuffer[pos - 1] != NEW_LINE)
         {
            throwInvalid();
         }
      }

      if (readingHeaders)
      {
         if (headerBytesCopyStart == -1)
         {
            headerBytesCopyStart = pos;
         }

         // Now the headers

         outer: while (true)
         {
            byte b = workingBuffer[pos++];

            switch (b)
            {
               case HEADER_SEPARATOR:
               {
                  if (inHeaderName)
                  {
                     byte[] data = new byte[pos - headerBytesCopyStart - 1];

                     System.arraycopy(workingBuffer, headerBytesCopyStart, data, 0, data.length);

                     headerName = new String(data);

                     inHeaderName = false;

                     headerBytesCopyStart = pos;

                     headerValueWhitespace = true;
                  }

                  whiteSpaceOnly = false;

                  break;
               }
               case NEW_LINE:
               {
                  if (whiteSpaceOnly)
                  {
                     // Headers are terminated by a blank line
                     readingHeaders = false;

                     break outer;
                  }

                  byte[] data = new byte[pos - headerBytesCopyStart - 1];

                  System.arraycopy(workingBuffer, headerBytesCopyStart, data, 0, data.length);

                  String headerValue = new String(data);

                  headers.put(headerName, headerValue);

                  if (headerName.equals(CONTENT_LENGTH_HEADER_NAME))
                  {
                     contentLength = Integer.parseInt(headerValue.toString());
                  }

                  whiteSpaceOnly = true;

                  headerBytesCopyStart = pos;

                  inHeaderName = true;

                  headerValueWhitespace = false;

                  break;
               }
               case SPACE:
               {
               }
               case TAB:
               {
                  if (TRIM_LEADING_HEADER_VALUE_WHITESPACE && headerValueWhitespace)
                  {
                     // trim off leading whitespace from header values.
                     // The STOMP spec examples seem to imply that whitespace should be trimmed although it is not
                     // explicit in the spec
                     // ActiveMQ + StompConnect also seem to trim whitespace from header values.
                     // Trimming is problematic though if the user has set a header with a value which deliberately
                     // has
                     // leading whitespace since
                     // this will be removed
                     headerBytesCopyStart++;
                  }

                  break;
               }
               default:
               {
                  whiteSpaceOnly = false;

                  headerValueWhitespace = false;
               }
            }
            if (pos == data)
            {
               // Run out of data

               return null;
            }
         }
      }

      // Now the body

      byte[] content = null;

      if (contentLength != -1)
      {
         if (pos + contentLength + 1 > data)
         {
            // Need more bytes
         }
         else
         {
            content = new byte[contentLength];

            System.arraycopy(workingBuffer, pos, content, 0, contentLength);

            pos += contentLength + 1;
         }
      }
      else
      {
         // Need to scan for terminating NUL

         if (bodyStart == -1)
         {
            bodyStart = pos;
         }

         while (pos < data)
         {
            if (workingBuffer[pos++] == 0)
            {
               content = new byte[pos - bodyStart - 1];

               System.arraycopy(workingBuffer, bodyStart, content, 0, content.length);

               break;
            }
         }
      }

      if (content != null)
      {
         if (data > pos)
         {
            if (workingBuffer[pos] == NEW_LINE) pos++;

            if (data > pos)
              // More data still in the buffer from the next packet
              System.arraycopy(workingBuffer, pos, workingBuffer, 0, data - pos);
         }

         data = data - pos;

         // reset

         StompFrame ret = new StompFrame(command, headers, content);

         init();

         return ret;
      }
      else
      {
         return null;
      }
   }

   public void throwInvalid() throws HornetQStompException
   {
      throw new HornetQStompException("Invalid STOMP frame: " + this.dumpByteArray(workingBuffer));
   }

   public void init()
   {
      pos = 0;

      command = null;

      headers = new HashMap<String, String>();

      this.headerBytesCopyStart = -1;

      readingHeaders = true;

      inHeaderName = true;

      headerValueWhitespace = false;

      headerName = null;

      whiteSpaceOnly = true;

      contentLength = -1;

      contentType = null;

      bodyStart = -1;
   }

   public void resizeWorking(final int newSize)
   {
      byte[] oldBuffer = workingBuffer;

      workingBuffer = new byte[newSize];

      System.arraycopy(oldBuffer, 0, workingBuffer, 0, oldBuffer.length);
   }

   public boolean tryIncrement(final int length)
   {
      if (pos + length >= data)
      {
         return false;
      }
      else
      {
         pos += length;

         return true;
      }
   }

   private String dumpByteArray(final byte[] bytes)
   {
      StringBuilder str = new StringBuilder();

      for (int i = 0; i < data; i++)
      {
         char b = (char)bytes[i];

         if (b < 33 || b > 136)
         {
            //Unreadable characters

            str.append(bytes[i]);
         }
         else
         {
            str.append(b);
         }

         if (i != bytes.length - 1)
         {
            str.append(",");
         }
      }

      return str.toString();
   }
}
