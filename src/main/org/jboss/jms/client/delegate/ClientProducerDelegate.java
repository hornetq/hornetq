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

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Destination;
import javax.jms.MessageFormatException;
import javax.jms.BytesMessage;
import javax.jms.MapMessage;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.jboss.jms.client.state.ProducerState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.delegate.ProducerDelegate;
import org.jboss.jms.delegate.ConnectionDelegate;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.messaging.newcore.impl.DestinationImpl;
import org.jboss.logging.Logger;

/**
 * The client-side Producer delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientProducerDelegate extends DelegateSupport<ProducerState> implements ProducerDelegate
{
   // Constants ------------------------------------------------------------------------------------

   private static final long serialVersionUID = -6976930316308905681L;
   private static final Logger log = Logger.getLogger(ClientProducerDelegate.class);

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // DelegateSupport overrides --------------------------------------------------------------------

   public void synchronizeWith(DelegateSupport nd) throws Exception
   {
      super.synchronizeWith(nd);

      ClientProducerDelegate newDelegate = (ClientProducerDelegate)nd;

      // synchronize server endpoint state

      // synchronize (recursively) the client-side state

      state.synchronizeWith(newDelegate.getState());
   }

   // ProducerDelegate implementation --------------------------------------------------------------

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void close() throws JMSException
   {
      return;
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public long closing(long sequence) throws JMSException
   {
      return -1;
   }

   public void setDestination(JBossDestination dest)
   {
      state.setDestination(dest);
   }

   public JBossDestination getDestination() throws JMSException
   {
      return (JBossDestination)state.getDestination();
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void send(JBossDestination destination, Message m, int deliveryMode,
                    int priority, long timeToLive) throws JMSException
   {
      send(destination, m, deliveryMode, priority, timeToLive, false);
   }


   public void send(JBossDestination destination, Message m, int deliveryMode, int priority, long timeToLive, boolean keepID) throws JMSException
   {

      // configure the message for sending, using attributes stored as metadata

      ProducerState producerState = getState();

      if (deliveryMode == -1)
      {
         // Use the delivery mode of the producer
         deliveryMode = producerState.getDeliveryMode();
         if (trace) { log.trace("Using producer's default delivery mode: " + deliveryMode); }
      }
      m.setJMSDeliveryMode(deliveryMode);

      if (priority == -1)
      {
         // Use the priority of the producer
         priority = producerState.getPriority();
         if (trace) { log.trace("Using producer's default priority: " + priority); }
      }
      if (priority < 0 || priority > 9)
      {
         throw new MessageFormatException("Invalid message priority (" + priority + "). " +
                                          "Valid priorities are 0-9");
      }
      m.setJMSPriority(priority);

      if (producerState.isDisableMessageTimestamp())
      {
         m.setJMSTimestamp(0l);
      }
      else
      {
         m.setJMSTimestamp(System.currentTimeMillis());
      }

      if (timeToLive == Long.MIN_VALUE)
      {
         // Use time to live value from producer
         timeToLive = producerState.getTimeToLive();
         if (trace) { log.trace("Using producer's default timeToLive: " + timeToLive); }
      }

      if (timeToLive == 0)
      {
         // Zero implies never expires
         m.setJMSExpiration(0);
      }
      else
      {
         m.setJMSExpiration(System.currentTimeMillis() + timeToLive);
      }

      if (destination == null)
      {
         // use destination from producer
         destination = (JBossDestination)producerState.getDestination();

         if (destination == null)
         {
            throw new UnsupportedOperationException("Destination not specified");
         }

         if (trace) { log.trace("Using producer's default destination: " + destination); }
      }
      else
      {
         // if a default destination was already specified then this must be same destination as
         // that specified in the arguments

         if (producerState.getDestination() != null &&
             !producerState.getDestination().equals(destination))
         {
            throw new UnsupportedOperationException("Where a default destination is specified " +
                                                    "for the sender and a destination is " +
                                                    "specified in the arguments to the send, " +
                                                    "these destinations must be equal");
         }
      }

      SessionState sessionState = (SessionState)producerState.getParent();

      // Generate the message id
      ConnectionState connectionState = (ConnectionState)sessionState.getParent();

      JBossMessage jbm;

      boolean foreign = false;

      //First convert from foreign message if appropriate
      if (!(m instanceof JBossMessage))
      {
         // it's a foreign message

         // JMS 1.1 Sect. 3.11.4: A provider must be prepared to accept, from a client,
         // a message whose implementation is not one of its own.

         // create a matching JBossMessage Type from JMS Type
         if (m instanceof BytesMessage)
         {
            jbm = new JBossBytesMessage((BytesMessage)m);
         }
         else if (m instanceof MapMessage)
         {
            jbm = new JBossMapMessage((MapMessage)m);
         }
         else if (m instanceof ObjectMessage)
         {
            jbm = new JBossObjectMessage((ObjectMessage)m);
         }
         else if (m instanceof StreamMessage)
         {
            jbm = new JBossStreamMessage((StreamMessage)m);
         }
         else if (m instanceof TextMessage)
         {
            jbm = new JBossTextMessage((TextMessage)m);
         }
         else
         {
            jbm = new JBossMessage(m);
         }

         //Set the destination on the original message
         m.setJMSDestination(destination);

         foreign = true;
      }
      else
      {
         jbm = (JBossMessage)m;
      }

      if (!keepID)
      {
         // Generate a new id
         long id = connectionState.getIdGenerator().getId((ConnectionDelegate)connectionState.getDelegate());

         jbm.getCoreMessage().setMessageID(id);

         //Set to null - this will cause the next call to getJMSMessageID() on the jbm to recalculate
         //it - need to do this to prevent any old cached value being retained

         jbm.setJMSMessageID(null);
      }

      if (foreign)
      {
         m.setJMSMessageID(jbm.getJMSMessageID());
      }

      jbm.setJMSDestination(destination);


      try
      {
         jbm.doBeforeSend();
      }
      catch (Exception e)
      {
         JMSException exthrown = new JMSException (e.toString());
         exthrown.initCause(e);
         throw exthrown;
      }

      JBossDestination dest = (JBossDestination)destination;

      //Set the destination on the core message - TODO temp for refactoring
      org.jboss.messaging.newcore.Destination coreDest =
         new DestinationImpl(dest.isQueue() ? "Queue" : "Topic", dest.getName(), dest.isTemporary());

      org.jboss.messaging.newcore.Message messageToSend = jbm.getCoreMessage();

      //FIXME - temp - for now we set destination as a header - should really be an attribute of the
      //send packet - along with scheduleddelivery time

      messageToSend.putHeader(org.jboss.messaging.newcore.Message.TEMP_DEST_HEADER_NAME, coreDest);

      //We copy *before* sending
      //TODO for now we always copy - for INVM we can optimise (like we did in 1.4) by doing lazy copying
      //of message, header and properties
      jbm.copyMessage();

      // we now invoke the send(Message) method on the session, which will eventually be fielded
      // by connection endpoint
      sessionState.getDelegate().send(messageToSend, false);
   }

   /**
    * This invocation should either be handled by the client-side interceptor chain or by the
    * server-side endpoint.
    */
   public void send(JBossMessage message) throws JMSException
   {
      throw new IllegalStateException("This invocation should not be handled here!");  
   }

   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      getState().setDeliveryMode(deliveryMode);
   }

   public int getDeliveryMode() throws JMSException
   {
      return getState().getDeliveryMode();
   }

   

   public boolean getDisableMessageID() throws JMSException
   {
      return getState().isDisableMessageID();
   }

   public void setDisableMessageID(boolean value) throws JMSException
   {
      getState().setDisableMessageID(value);   
   }

   public boolean getDisableMessageTimestamp() throws JMSException
   {
      return getState().isDisableMessageTimestamp();
   }

   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      getState().setDisableMessageTimestamp(value);
   }

   public void setPriority(int priotiy) throws JMSException
   {
      state.setPriority(priotiy);
   }

   public int getPriority() throws JMSException
   {
      return state.getPriority();
   }

   public long getTimeToLive() throws JMSException
   {
        return state.getTimeToLive();
   }


   public void setTimeToLive(long timeToLive) throws JMSException
   {
      state.setTimeToLive(timeToLive);
   }

   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "ProducerDelegate[" + System.identityHashCode(this) + ", ID=" + id + "]";
   }

   // Protected ------------------------------------------------------------------------------------
   
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner Classes --------------------------------------------------------------------------------

}
