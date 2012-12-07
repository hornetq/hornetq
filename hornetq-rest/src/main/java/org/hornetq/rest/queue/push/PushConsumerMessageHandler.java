package org.hornetq.rest.queue.push;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.rest.HornetQRestLogger;

public class PushConsumerMessageHandler implements MessageHandler
{
   private ClientSession session;
   private PushConsumer pushConsumer;

   PushConsumerMessageHandler(PushConsumer pushConsumer, ClientSession session)
   {
      this.pushConsumer = pushConsumer;
      this.session = session;
   }

   @Override
   public void onMessage(ClientMessage clientMessage)
   {
      HornetQRestLogger.LOGGER.debug(this + ": receiving " + clientMessage);

      try
      {
         clientMessage.acknowledge();
         HornetQRestLogger.LOGGER.debug(this + ": acknowledged " + clientMessage);
      }
      catch (HornetQException e)
      {
         throw new RuntimeException(e.getMessage(), e);
      }

      HornetQRestLogger.LOGGER.debug(this + ": pushing " + clientMessage + " via " + pushConsumer.getStrategy());
      boolean acknowledge = pushConsumer.getStrategy().push(clientMessage);

      if (acknowledge)
      {
         try
         {
            HornetQRestLogger.LOGGER.debug("Acknowledging: " + clientMessage.getMessageID());
            session.commit();
            return;
         }
         catch (HornetQException e)
         {
            throw new RuntimeException(e);
         }
      }
      else
      {
         try
         {
            session.rollback();
         }
         catch (HornetQException e)
         {
            throw new RuntimeException(e.getMessage(), e);
         }
         if (pushConsumer.getRegistration().isDisableOnFailure())
         {
            HornetQRestLogger.LOGGER.errorPushingMessage(pushConsumer.getRegistration().getTarget());
            pushConsumer.disableFromFailure();
            return;
         }
      }
   }
}
