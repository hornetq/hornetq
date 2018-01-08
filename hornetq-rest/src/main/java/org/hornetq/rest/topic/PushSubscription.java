package org.hornetq.rest.topic;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.jms.client.ConnectionFactoryOptions;
import org.hornetq.rest.HornetQRestLogger;
import org.hornetq.rest.queue.push.PushConsumer;
import org.hornetq.rest.queue.push.PushStore;
import org.hornetq.rest.queue.push.xml.PushRegistration;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class PushSubscription extends PushConsumer
{
   public PushSubscription(ClientSessionFactory factory, String destination, String id, PushRegistration registration, PushStore store, ConnectionFactoryOptions jmsOptions)
   {
      super(factory, destination, id, registration, store, jmsOptions);
   }

   @Override
   public void disableFromFailure()
   {
      super.disableFromFailure();
      if (registration.isDurable()) deleteSubscriberQueue();
   }

   protected void deleteSubscriberQueue()
   {
      String subscriptionName = registration.getDestination();
      ClientSession session = null;
      try
      {
         session = factory.createSession();

         session.deleteQueue(subscriptionName);
      }
      catch (HornetQException e)
      {
         HornetQRestLogger.LOGGER.errorDeletingSubscriberQueue(e);
      }
      finally
      {
         try
         {
            if (session != null)
               session.close();
         }
         catch (HornetQException e)
         {
         }
      }
   }
}
