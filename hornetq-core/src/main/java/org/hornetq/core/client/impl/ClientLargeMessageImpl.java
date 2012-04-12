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

package org.hornetq.core.client.impl;

import java.io.IOException;
import java.io.OutputStream;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.core.buffers.impl.ResetLimitWrappedHornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * ClientLargeMessageImpl is only created when receiving large messages.
 * <p>
 * At the time of sending a regular Message is sent as we won't know the message is considered large
 * until the buffer is filled up or the user set a streaming.
 * @author clebertsuconic
 */
public final class ClientLargeMessageImpl extends ClientMessageImpl implements ClientLargeMessageInternal
{

   // Used only when receiving large messages
   private LargeMessageController largeMessageController;

   private long largeMessageSize;

   /**
    * @return the largeMessageSize
    */
   public long getLargeMessageSize()
   {
      return largeMessageSize;
   }

   /**
    * @param largeMessageSize the largeMessageSize to set
    */
   public void setLargeMessageSize(long largeMessageSize)
   {
      this.largeMessageSize = largeMessageSize;
   }

   // we only need this constructor as this is only used at decoding large messages on client
   public ClientLargeMessageImpl()
   {
      super();
   }

   // Public --------------------------------------------------------

   @Override
   public int getEncodeSize()
   {
      if (bodyBuffer != null)
      {
         return super.getEncodeSize();
      }
      else
      {
         return DataConstants.SIZE_INT + DataConstants.SIZE_INT + getHeadersAndPropertiesEncodeSize();
      }
   }

   /**
    * @return the largeMessage
    */
   @Override
   public boolean isLargeMessage()
   {
      return true;
   }

   public void setLargeMessageController(final LargeMessageController controller)
   {
      largeMessageController = controller;
   }

   @Override
   public HornetQBuffer getBodyBuffer()
   {
      checkBuffer();

      return bodyBuffer;
   }

   @Override
   public int getBodySize()
   {
      return getLongProperty(Message.HDR_LARGE_BODY_SIZE).intValue();
   }

   public LargeMessageController getLargeMessageController()
   {
      return largeMessageController;
   }

   @Override
   public void saveToOutputStream(final OutputStream out) throws HornetQException
   {
      if (bodyBuffer != null)
      {
         // The body was rebuilt on the client, so we need to behave as a regular message on this case
         super.saveToOutputStream(out);
      }
      else
      {
         largeMessageController.saveBuffer(out);
      }
   }

   @Override
   public void setOutputStream(final OutputStream out) throws HornetQException
   {
      if (bodyBuffer != null)
      {
         super.setOutputStream(out);
      }
      else
      {
         largeMessageController.setOutputStream(out);
      }
   }

   @Override
   public boolean waitOutputStreamCompletion(final long timeMilliseconds) throws HornetQException
   {
      if (bodyBuffer != null)
      {
         return super.waitOutputStreamCompletion(timeMilliseconds);
      }
      else
      {
         return largeMessageController.waitCompletion(timeMilliseconds);
      }
   }

   @Override
   public void discardBody()
   {
      if (bodyBuffer != null)
      {
         super.discardBody();
      }
      else
      {
         largeMessageController.discardUnusedPackets();
      }
   }

   private void checkBuffer()
   {
      if (bodyBuffer == null)
      {

         long bodySize = this.largeMessageSize + BODY_OFFSET;
         if (bodySize > Integer.MAX_VALUE)
         {
            bodySize = Integer.MAX_VALUE;
         }
         createBody((int)bodySize);

         bodyBuffer = new ResetLimitWrappedHornetQBuffer(BODY_OFFSET, buffer, this);

         try
         {
            largeMessageController.saveBuffer(new HornetQOutputStream(bodyBuffer));
         }
         catch (HornetQException e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }
      }
   }

   // Inner classes -------------------------------------------------

   protected static class HornetQOutputStream extends OutputStream
   {
      HornetQBuffer bufferOut;

      HornetQOutputStream(HornetQBuffer out)
      {
         this.bufferOut = out;
      }

      @Override
      public void write(int b) throws IOException
      {
         bufferOut.writeByte((byte)(b & 0xff));
      }
   }
}
