/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
package org.jboss.jms.server.endpoint.advised;

import static org.jboss.messaging.core.remoting.Assert.assertValidID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SENDTRANSACTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_SETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STARTCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.MSG_STOPCONNECTION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CLOSING;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_CREATESESSION;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETCLIENTID;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_GETPREPAREDTRANSACTIONS;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.REQ_IDBLOCK;

import javax.jms.JMSException;

import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.delegate.ConnectionEndpoint;
import org.jboss.jms.delegate.IDBlock;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.exception.MessagingJMSException;
import org.jboss.jms.tx.MessagingXid;
import org.jboss.jms.tx.TransactionRequest;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.AbstractPacket;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionRequest;
import org.jboss.messaging.core.remoting.wireformat.CreateSessionResponse;
import org.jboss.messaging.core.remoting.wireformat.GetClientIDResponse;
import org.jboss.messaging.core.remoting.wireformat.GetPreparedTransactionsResponse;
import org.jboss.messaging.core.remoting.wireformat.IDBlockRequest;
import org.jboss.messaging.core.remoting.wireformat.IDBlockResponse;
import org.jboss.messaging.core.remoting.wireformat.JMSExceptionMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.SendTransactionMessage;
import org.jboss.messaging.core.remoting.wireformat.SetClientIDMessage;

/**
 * The server-side advised instance corresponding to a Connection. It is bound to the AOP
 * Dispatcher's map.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionAdvised extends AdvisedSupport implements ConnectionEndpoint
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConnectionEndpoint endpoint;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionAdvised(ConnectionEndpoint endpoint)
   {
      this.endpoint = endpoint;
   }

   // ConnectionEndpoint implementation -----------------------------

   public void close() throws JMSException
   {
      endpoint.close();
   }

   public long closing(long sequence) throws JMSException
   {
      return endpoint.closing(sequence);
   }

   public SessionDelegate createSessionDelegate(boolean transacted,
                                                int acknowledgmentMode,
                                                boolean isXA) throws JMSException
   {
      return endpoint.createSessionDelegate(transacted, acknowledgmentMode, isXA);
   }

   public String getClientID() throws JMSException
   {
      return endpoint.getClientID();
   }

   public void setClientID(String id) throws JMSException
   {
      endpoint.setClientID(id);
   }

   public void start() throws JMSException
   {
      endpoint.start();
   }

   public void stop() throws JMSException
   {
      endpoint.stop();
   }

   public void sendTransaction(TransactionRequest request,
                               boolean checkForDuplicates) throws JMSException
   {
      endpoint.sendTransaction(request, checkForDuplicates);
   }

   public MessagingXid[] getPreparedTransactions() throws JMSException
   {
      return endpoint.getPreparedTransactions();
   }
   
   public IDBlock getIdBlock(int size) throws JMSException
   {
      return endpoint.getIdBlock(size);
   }
   
   // Public --------------------------------------------------------

   public Object getEndpoint()
   {
      return endpoint;
   }

   public String toString()
   {
      return "ConnectionAdvised->" + endpoint;
   }

   // Protected -----------------------------------------------------

   // Package Private -----------------------------------------------

   // Private -------------------------------------------------------

   // Inner Classes -------------------------------------------------

   public class ConnectionAdvisedPacketHandler implements PacketHandler
   {

      private final String id;

      public ConnectionAdvisedPacketHandler(String id)
      {
         assertValidID(id);
         
         this.id = id;
      }
      
      public String getID()
      {
         return id;
      }

      public void handle(AbstractPacket packet, PacketSender sender)
      {
         try
         {
            AbstractPacket response = null;

            PacketType type = packet.getType();
            if (type == REQ_CREATESESSION)
            {
               CreateSessionRequest request = (CreateSessionRequest) packet;
               ClientSessionDelegate sessionDelegate = (ClientSessionDelegate) createSessionDelegate(
                     request.isTransacted(), request.getAcknowledgementMode(),
                     request.isXA());

               response = new CreateSessionResponse(sessionDelegate.getID(),
                     sessionDelegate.getDupsOKBatchSize(), sessionDelegate
                           .isStrictTck());
            } else if (type == REQ_IDBLOCK)
            {
               IDBlockRequest request = (IDBlockRequest) packet;
               IDBlock idBlock = getIdBlock(request.getSize());

               response = new IDBlockResponse(idBlock.getLow(), idBlock
                     .getHigh());
            } else if (type == MSG_STARTCONNECTION)
            {
               start();
            } else if (type == MSG_STOPCONNECTION)
            {
               stop();

               response = new NullPacket();
            } else if (type == REQ_CLOSING)
            {
               ClosingRequest request = (ClosingRequest) packet;
               long id = closing(request.getSequence());

               response = new ClosingResponse(id);
            } else if (type == MSG_CLOSE)
            {
               close();

               response = new NullPacket();
            } else if (type == MSG_SENDTRANSACTION)
            {
               SendTransactionMessage message = (SendTransactionMessage) packet;
               sendTransaction(message.getTransactionRequest(), message
                     .checkForDuplicates());

               response = new NullPacket();
            } else if (type == REQ_GETPREPAREDTRANSACTIONS)
            {
               MessagingXid[] xids = getPreparedTransactions();

               response = new GetPreparedTransactionsResponse(xids);
            } else if (type == REQ_GETCLIENTID)
            {
               response = new GetClientIDResponse(getClientID());
            } else if (type == MSG_SETCLIENTID)
            {
               SetClientIDMessage message = (SetClientIDMessage) packet;
               setClientID(message.getClientID());

               response = new NullPacket();
            } else
            {
               response = new JMSExceptionMessage(new MessagingJMSException(
                     "Unsupported packet for browser: " + packet));
            }

            // reply if necessary
            if (response != null)
            {
               response.normalize(packet);
               sender.send(response);
            }

         } catch (JMSException e)
         {
            JMSExceptionMessage message = new JMSExceptionMessage(e);
            message.normalize(packet);
            sender.send(message);
         }
      }

      @Override
      public String toString()
      {
         return "ConnectionAdvisedPacketHandler[id=" + id + "]";
      }
   }
}
