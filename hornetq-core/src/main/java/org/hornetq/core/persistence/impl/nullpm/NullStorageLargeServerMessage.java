/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.core.persistence.impl.nullpm;

import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;

/**
 * A NullStorageLargeServerMessage
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created 30-Sep-08 1:51:42 PM
 *
 *
 */
public class NullStorageLargeServerMessage extends ServerMessageImpl implements LargeServerMessage
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public NullStorageLargeServerMessage()
   {
      super();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#release()
    */
   public void releaseResources()
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#addBytes(byte[])
    */
   public synchronized void addBytes(final byte[] bytes)
   {
      if (buffer == null)
      {
         buffer = HornetQBuffers.dynamicBuffer(bytes.length);
      }

      // expand the buffer
      buffer.writeBytes(bytes);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#deleteFile()
    */
   public void deleteFile() throws Exception
   {
      // nothing to be done here.. we don really have a file on this Storage
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#complete()
    */
   public void complete() throws Exception
   {
      // nothing to be done here.. we don really have a file on this Storage

   }

   @Override
   public boolean isLargeMessage()
   {
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#getLinkedMessage()
    */
   public LargeServerMessage getLinkedMessage()
   {
      return null;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setLinkedMessage(org.hornetq.core.server.LargeServerMessage)
    */
   public void setLinkedMessage(final LargeServerMessage message)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#isComplete()
    */
   public boolean isComplete()
   {
      // nothing to be done on null persistence
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setComplete(boolean)
    */
   public void setComplete(final boolean isComplete)
   {
      // nothing to be done on null persistence
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#isFileExists()
    */
   public boolean isFileExists() throws Exception
   {
      // There are no real files on null persistence
      return true;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#decrementDelayDeletionCount()
    */
   public void decrementDelayDeletionCount()
   {

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#incrementDelayDeletionCount()
    */
   public void incrementDelayDeletionCount()
   {

   }

   @Override
   public synchronized int getEncodeSize()
   {
      return getHeadersAndPropertiesEncodeSize();
   }

   @Override
   public String toString()
   {
      return "LargeServerMessage[messageID=" + messageID + ", durable=" + durable + ", address=" + getAddress()  + ",properties=" + properties.toString() + "]";
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setPaged()
    */
   public void setPaged()
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setPendingRecordID(long)
    */
   public void setPendingRecordID(long pendingRecordID)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#getPendingRecordID()
    */
   public long getPendingRecordID()
   {
      return -1;
   }

   /*
    * (non-Javadoc)
    * @see org.hornetq.core.server.LargeServerMessage#setReplicationSync(boolean)
    */
   @Override
   public void setReplicationSync(boolean sync)
   {
      // TODO Auto-generated method stub

   }

   @Override
   public SequentialFile getFile()
   {
      return null;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
