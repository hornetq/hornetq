package org.hornetq.core.protocol.proton;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;

/**
 * handles an amqp Coordinator to deal with transaction boundaries etc
 */
public class TransactionHandler implements ProtonDeliveryHandler
{
   private final ProtonRemotingConnection connection;
   private final Coordinator coordinator;
   private final ProtonProtocolManager protonProtocolManager;
   private final ProtonSession protonSession;
   private final HornetQBuffer buffer;

   public TransactionHandler(ProtonRemotingConnection connection, Coordinator coordinator, ProtonProtocolManager protonProtocolManager, ProtonSession protonSession)
   {
      this.connection = connection;
      this.coordinator = coordinator;
      this.protonProtocolManager = protonProtocolManager;
      this.protonSession = protonSession;
      buffer = connection.createBuffer(1024);
   }

   @Override
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      Receiver receiver = null;
      try
      {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable())
         {
            return;
         }

         protonProtocolManager.handleTransaction(receiver, buffer, delivery, protonSession);

      }
      catch (Exception e)
      {
         e.printStackTrace();
         Rejected rejected = new Rejected();
         ErrorCondition condition = new ErrorCondition();
         condition.setCondition(Symbol.valueOf("failed"));
         condition.setDescription(e.getMessage());
         rejected.setError(condition);
         delivery.disposition(rejected);
      }
   }

   @Override
   public void checkState()
   {
      //noop
   }

   @Override
   public void close() throws HornetQAMQPException
   {
      //noop
   }
}
