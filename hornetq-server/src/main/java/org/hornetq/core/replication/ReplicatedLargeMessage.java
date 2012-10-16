/**
 *
 */
package org.hornetq.core.replication;

import org.hornetq.core.server.LargeServerMessage;

/**
 *
 */
public interface ReplicatedLargeMessage
{
   /**
    * @see LargeServerMessage#setDurable(boolean)
    */
   void setDurable(boolean b);

   /**
    * @see LargeServerMessage#setMessageID(long)
    */
   void setMessageID(long id);

   /**
    * @see LargeServerMessage#releaseResources()
    */
   void releaseResources();

   /**
    * @see LargeServerMessage#deleteFile()
    */
   void deleteFile() throws Exception;

   /**
    * @see LargeServerMessage#addBytes(byte[])
    */
   void addBytes(byte[] body) throws Exception;

}
