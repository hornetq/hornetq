package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_QUEUEQUERY_RESP;

/**
 * 
 * A SessionQueueQueryResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionQueueQueryResponseMessage extends AbstractPacket
{
   private boolean exists;
   
   private boolean durable;
   
   private boolean temporary;
   
   private int maxSize;
   
   private int consumerCount;
   
   private int messageCount;
   
   private String filterString;
   
   private String address;
   
   //etc
   
   public SessionQueueQueryResponseMessage(boolean durable, boolean temporary, int maxSize, int consumerCount,
                             int messageCount, String filterString, String address)
   {
      super(SESS_QUEUEQUERY_RESP);
      
      this.exists = true;
      
      this.durable = durable;
      
      this.temporary = temporary;
      
      this.maxSize = maxSize;
      
      this.consumerCount = consumerCount;
      
      this.messageCount = messageCount;
      
      this.filterString = filterString;
      
      this.address = address;
   }
   
   public SessionQueueQueryResponseMessage()
   {
      super(SESS_QUEUEQUERY_RESP);
      
      this.exists = false;
   }
   
   public boolean isExists()
   {
      return exists;
   }
   
   public boolean isDurable()
   {
      return durable;
   }
   
   public boolean isTemporary()
   {
      return temporary;
   }
   
   public int getMaxSize()
   {
      return maxSize;
   }
   
   public int getConsumerCount()
   {
      return consumerCount;
   }
   
   public int getMessageCount()
   {
      return messageCount;
   }
   
   public String getFilterString()
   {
      return filterString;
   }
   
   public String getAddress()
   {
      return address;
   }
   
}
