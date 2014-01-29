/**
 *
 */
package org.hornetq.core.replication;

/**
 * {@link LargeServerMessage} methods used by the {@link ReplicationEndpoint}.
 * <p/>
 * In practice a subset of the methods necessary to have a {@link LargeServerMessage}
 *
 * @see LargeServerMessageInSync
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
