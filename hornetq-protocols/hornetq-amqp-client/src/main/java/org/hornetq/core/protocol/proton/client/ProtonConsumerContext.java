/*
 * Copyright 2005-2014 Red Hat, Inc.
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

package org.hornetq.core.protocol.proton.client;

import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.hornetq.core.client.impl.ClientSessionImpl;
import org.hornetq.spi.core.remoting.ConsumerContext;

/**
 * @author Clebert Suconic
 */

public class ProtonConsumerContext extends ConsumerContext
{
   private final Receiver receiver;

   private final ClientSessionImpl clientSession;

   public ProtonConsumerContext(Receiver receiver, ClientSessionImpl clientSession)
   {
      this.receiver = receiver;
      this.clientSession = clientSession;
   }

   public Receiver getReceiver()
   {
      return receiver;
   }

   public ClientSessionImpl getClientSession()
   {
      return clientSession;
   }

   public void processUpdates()
   {
      Delivery incoming = null;
      do
      {
         incoming = receiver.current();
         if (incoming != null && incoming.isReadable() && !incoming.isPartial())
         {
            incoming.clear();
            try
            {
               AMQPMessage msg = new AMQPMessage(incoming);
               msg.setFlowControlSize(1);
               // TODO handle message's body
               clientSession.handleReceiveMessage(this, msg);
            }
            catch (Exception e)
            {
               // TODO: redelivery
               e.printStackTrace();
            }
         }
         else
         {
            incoming = null;
         }
         receiver.advance();
      } while (incoming != null);
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ProtonConsumerContext that = (ProtonConsumerContext) o;

      if (receiver != null ? !receiver.equals(that.receiver) : that.receiver != null) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      return receiver != null ? receiver.hashCode() : 0;
   }
}
