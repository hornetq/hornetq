package org.hornetq.rest.topic;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.rest.queue.AcknowledgedQueueConsumer;
import org.hornetq.rest.queue.DestinationServiceManager;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class AcknowledgedSubscriptionResource extends AcknowledgedQueueConsumer implements Subscription
{
   private boolean durable;
   private long timeout;
   private boolean deleteWhenIdle;

   public AcknowledgedSubscriptionResource(ClientSessionFactory factory, String destination, String id, DestinationServiceManager serviceManager, String selector, boolean durable, Long timeout)
           throws HornetQException
   {
      super(factory, destination, id, serviceManager, selector);
      this.timeout = timeout;
      this.durable = durable;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public void setDurable(boolean durable)
   {
      this.durable = durable;
   }

   public long getTimeout()
   {
      return timeout;
   }

   public void setTimeout(long timeout)
   {
      this.timeout = timeout;
   }

   public boolean isDeleteWhenIdle()
   {
      return deleteWhenIdle;
   }

   public void setDeleteWhenIdle(boolean deleteWhenIdle)
   {
      this.deleteWhenIdle = deleteWhenIdle;
   }
}
