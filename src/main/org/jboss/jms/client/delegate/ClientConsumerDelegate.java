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
package org.jboss.jms.client.delegate;

import java.io.DataInputStream;
import java.io.DataOutputStream;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.jboss.jms.client.api.Consumer;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.client.container.ClientConsumer;
import org.jboss.jms.client.remoting.CallbackManager;
import org.jboss.jms.exception.MessagingShutdownException;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.Destination;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.remoting.PacketDispatcher;
import org.jboss.messaging.core.remoting.wireformat.ChangeRateMessage;
import org.jboss.messaging.core.remoting.wireformat.CloseMessage;
import org.jboss.messaging.core.remoting.wireformat.ClosingRequest;
import org.jboss.messaging.core.remoting.wireformat.ClosingResponse;
import org.jboss.messaging.core.remoting.Client;

/**
 * The client-side Consumer delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientConsumerDelegate extends CommunicationSupport<ClientConsumerDelegate> implements Consumer
{
   // Constants ------------------------------------------------------------------------------------

	private static final long serialVersionUID = 3253922610778321868L;

	private static final Logger log = Logger.getLogger(ClientConsumerDelegate.class);

   // Attributes -----------------------------------------------------------------------------------

	private ClientSession session;
   private int bufferSize;
   private int maxDeliveries;
   private long redeliveryDelay;

   // State attributes -----------------------------------------------------------------------------

   private String consumerID;
   private Destination destination;
   private String selector;
   private String subscriptionName;
   private boolean noLocal;
   private boolean isConnectionConsumer;
   private ClientConsumer clientConsumer;
   private boolean storingDeliveries;
   
   

   
   
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------
   public ClientConsumerDelegate(String objectID, int bufferSize, int maxDeliveries, long redeliveryDelay)
   {
      super(objectID);
      this.bufferSize = bufferSize;
      this.maxDeliveries = maxDeliveries;
      this.redeliveryDelay = redeliveryDelay;
   }

   public ClientConsumerDelegate(ClientSession session, String objectID, int bufferSize, int maxDeliveries, long redeliveryDelay,
         Destination dest,
         String selector, boolean noLocal, String subscriptionName, String consumerID,
         boolean isCC)
   {
      super(objectID);
      this.session = session;
      this.bufferSize = bufferSize;
      this.maxDeliveries = maxDeliveries;
      this.redeliveryDelay = redeliveryDelay;
      this.destination = dest;
      this.selector = selector;
      this.noLocal = noLocal;
      this.subscriptionName = subscriptionName;
      this.consumerID = consumerID;
      this.isConnectionConsumer = isCC;
   }

   public ClientConsumerDelegate()
   {
   }

   // DelegateSupport overrides --------------------------------------------------------------------

   @Override
   protected byte getVersion()
   {
      return session.getConnection().getVersion();
   }

   protected Client getClient()
   {
      return this.session.getConnection().getClient();
   }
   
   public void synchronizeWith(ClientConsumerDelegate nd) throws Exception
   {
      log.trace(this + " synchronizing with " + nd);

      super.synchronizeWith(nd);

      ClientConsumerDelegate newDelegate = (ClientConsumerDelegate)nd;

      // synchronize the delegates

      bufferSize = newDelegate.getBufferSize();
      maxDeliveries = newDelegate.getMaxDeliveries();

   }

   // Closeable implementation ---------------------------------------------------------------------

   public void close() throws JMSException
   {
      sendBlocking(new CloseMessage());
   }


   public long closing(long sequence) throws JMSException
   {
      try
      {

         // We make sure closing is called on the ServerConsumerEndpoint.
         // This returns us the last delivery id sent

         long lastDeliveryId = invokeClosing(sequence);

         // First we call close on the ClientConsumer which waits for onMessage invocations
         // to complete and the last delivery to arrive
         getClientConsumer().close(lastDeliveryId);

         session.removeCallbackHandler(getClientConsumer());

         CallbackManager cm = session.getConnection().getRemotingConnection().getCallbackManager();
         cm.unregisterHandler(getConsumerID());

         PacketDispatcher.client.unregister(getConsumerID());

         //And then we cancel any messages still in the message callback handler buffer
         getClientConsumer().cancelBuffer();

         return lastDeliveryId;

      }
      catch (Exception proxiedException)
      {
         // if MessagingServer is shutdown or
         // if there is no failover in place... we just close the consumerState as well
         if (proxiedException instanceof MessagingShutdownException /* ||
                 (connectionState.getFailoverCommandCenter() == null ) */ )


         {
            if (!getClientConsumer().isClosed())
            {
               getClientConsumer().close(-1);
            }
         }
         JMSException ex = new JMSException(proxiedException.toString());
         ex.initCause(proxiedException);
         throw ex;
      }
   }

   private long invokeClosing(long sequence) throws JMSException
   {
      ClosingRequest request = new ClosingRequest(sequence);
      ClosingResponse response = (ClosingResponse) sendBlocking(request);
      return response.getID();
   }

   // ConsumerDelegate implementation --------------------------------------------------------------

   public void changeRate(float newRate) throws JMSException
   {
      sendOneWay(new ChangeRateMessage(newRate));
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public MessageListener getMessageListener()
   {
      return getClientConsumer().getMessageListener();
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public Message receive(long timeout) throws JMSException
   {
      return getClientConsumer().receive(timeout);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void setMessageListener(MessageListener listener) throws JMSException
   {
      getClientConsumer().setMessageListener(listener);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public boolean getNoLocal()
   {
      return this.noLocal;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public Destination getDestination()
   {
      return this.destination;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public String getMessageSelector()
   {
      return this.selector;
   }

   // Streamable implementation ----------------------------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      super.read(in);

      bufferSize = in.readInt();

      maxDeliveries = in.readInt();
      
      redeliveryDelay = in.readLong();
   }

   public void write(DataOutputStream out) throws Exception
   {
      super.write(out);

      out.writeInt(bufferSize);

      out.writeInt(maxDeliveries);
      
      out.writeLong(redeliveryDelay);
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ConsumerDelegate[" + System.identityHashCode(this) + ", ID=" + id + "]";
   }

   public int getBufferSize()
   {
      return bufferSize;
   }

   public int getMaxDeliveries()
   {
      return maxDeliveries;
   }
   
   public long getRedeliveryDelay()
   {
   	return redeliveryDelay;
   }
   
   public String getConsumerID()
   {
      return consumerID;
   }

   public boolean isConnectionConsumer()
   {
      return isConnectionConsumer;
   }

   public void setClientConsumer(ClientConsumer handler)
   {
      this.clientConsumer = handler;
   }

   public ClientConsumer getClientConsumer()
   {
      return clientConsumer;
   }

   public String getSubscriptionName()
   {
      return subscriptionName;
   }

   public void setSubscriptionName(String subscriptionName)
   {
      this.subscriptionName = subscriptionName;
   }

   public boolean isStoringDeliveries()
   {
      return storingDeliveries;
   }
   
   public boolean isShouldAck()
   {
      //If e are a non durable subscriber to a topic then there is no need
      //to send acks to the server - we wouldn't have stored them on the server side anyway
      
      return !(destination.getType() == DestinationType.TOPIC && subscriptionName == null);      
   }

  
   
   // Protected ------------------------------------------------------------------------------------

   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner Classes --------------------------------------------------------------------------------

}
