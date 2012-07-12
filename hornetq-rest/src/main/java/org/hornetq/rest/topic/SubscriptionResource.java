package org.hornetq.rest.topic;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.rest.queue.DestinationServiceManager;
import org.hornetq.rest.queue.QueueConsumer;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SubscriptionResource extends QueueConsumer implements Subscription
{
   protected boolean durable;
   protected long timeout;
   private boolean deleteWhenIdle;

   public SubscriptionResource(ClientSessionFactory factory, String destination, String id, DestinationServiceManager serviceManager, String selector, boolean durable, long timeout)
           throws HornetQException
   {
      super(factory, destination, id, serviceManager, selector);
      this.durable = durable;
      this.timeout = timeout;
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
