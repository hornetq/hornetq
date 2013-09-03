/*
* JBoss, Home of Professional Open Source.
* Copyright 2010, Red Hat, Inc., and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.hornetq.core.protocol.proton;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.transaction.Coordinator;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.protocol.proton.exceptions.HornetQAMQPException;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.QueueQueryResult;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         <p/>
 *         handles incoming messages via a Proton Receiver and forwards them to HornetQ
 */
public class ProtonProducer implements ProtonDeliveryHandler
{
   private final ProtonRemotingConnection connection;

   private final ProtonSession protonSession;

   private final ProtonProtocolManager protonProtocolManager;

   private final Receiver receiver;

   private final String address;

   private HornetQBuffer buffer;

   public ProtonProducer(ProtonRemotingConnection connection, ProtonSession protonSession, ProtonProtocolManager protonProtocolManager, Receiver receiver)
   {
      this.connection = connection;
      this.protonSession = protonSession;
      this.protonProtocolManager = protonProtocolManager;
      this.receiver = receiver;
      this.address = ((Target) receiver.getRemoteTarget()).getAddress();
      buffer = connection.createBuffer(1024);
   }

   /*
   * called when Proton receives a message to be delivered via a Delivery.
   *
   * This may be called more than once per deliver so we have to cache the buffer until we have received it all.
   *
   * */
   public void onMessage(Delivery delivery) throws HornetQAMQPException
   {
      Receiver receiver;
      try
      {
         receiver = ((Receiver) delivery.getLink());

         if (!delivery.isReadable())
         {
            return;
         }

         protonProtocolManager.handleMessage(receiver, buffer, delivery, connection, protonSession, address);

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
      //no op
   }

   @Override
   public void close() throws HornetQAMQPException
   {
      protonSession.removeProducer(receiver);
   }

   public void init() throws HornetQAMQPException
   {
      org.apache.qpid.proton.amqp.messaging.Target target = (org.apache.qpid.proton.amqp.messaging.Target) receiver.getRemoteTarget();
      if (target.getDynamic())
      {
         //if dynamic we have to create the node (queue) and set the address on the target, the node is temporary and
         // will be deleted on closing of the session
         SimpleString queue = new SimpleString(java.util.UUID.randomUUID().toString());
         try
         {
            protonSession.getServerSession().createQueue(queue, queue, null, true, false);
         }
         catch (Exception e)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorCreatingTemporaryQueue(e.getMessage());
         }
         target.setAddress(queue.toString());
      }
      else
      {
         //if not dynamic then we use the targets address as the address to forward the messages to, however there has to
         //be a queue bound to it so we nee to check this.
         String address = target.getAddress();
         if (address == null)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.targetAddressNotSet();
         }
         try
         {
            QueueQueryResult queryResult = protonSession.getServerSession().executeQueueQuery(new SimpleString(address));
            if (!queryResult.isExists())
            {
               throw HornetQAMQPProtocolMessageBundle.BUNDLE.addressDoesntExist();
            }
         }
         catch (Exception e)
         {
            throw HornetQAMQPProtocolMessageBundle.BUNDLE.errorFindingTemporaryQueue(e.getMessage());
         }
      }
   }
}
