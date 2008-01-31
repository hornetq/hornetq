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
package org.jboss.jms.client.impl;

import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.jboss.jms.client.api.ClientProducer;
import org.jboss.jms.client.api.ClientSession;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.jms.message.JBossBytesMessage;
import org.jboss.jms.message.JBossMapMessage;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.message.JBossObjectMessage;
import org.jboss.jms.message.JBossStreamMessage;
import org.jboss.jms.message.JBossTextMessage;
import org.jboss.messaging.core.DestinationType;
import org.jboss.messaging.core.impl.DestinationImpl;
import org.jboss.messaging.util.Logger;

/**
 * The client-side Producer delegate class.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ClientProducerImpl implements ClientProducer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ClientProducerImpl.class);

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();
   
   private ClientSession session;
   
   private JBossDestination destination;

   private boolean disableMessageID = false;
   
   private boolean disableMessageTimestamp = false;
   
   private int priority = 4;
   
   private long timeToLive = 0;
   
   private int deliveryMode = DeliveryMode.PERSISTENT;
   
   private String id;
   
   private volatile boolean closed;
   
   // Static ---------------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------
      
   public ClientProducerImpl(ClientSession session, JBossDestination destination)
   {
      this.session = session;
      
      this.destination = destination;
      
      this.id = UUID.randomUUID().toString();
   }

   public String getID()
   {
      return id;
   }

   public synchronized void close() throws JMSException
   {
      if (closed)
      {
         return;         
      }
      session.removeProducer(this);
      
      closed = true;
   }

   public synchronized void closing() throws JMSException
   {    
   }

   public JBossDestination getDestination() throws JMSException
   {
      checkClosed();
      
      return this.destination;
   }

   public void send(JBossDestination destination, Message m, int deliveryMode, int priority,
                    long timeToLive) throws JMSException
   {
      checkClosed();
      
      // configure the message for sending, using attributes stored as metadata

      if (deliveryMode == -1)
      {
         // Use the delivery mode of the producer
         deliveryMode = getDeliveryMode();
         if (trace) { log.trace("Using producer's default delivery mode: " + deliveryMode); }
      }
      m.setJMSDeliveryMode(deliveryMode);

      if (priority == -1)
      {
         // Use the priority of the producer
         priority = getPriority();
         if (trace) { log.trace("Using producer's default priority: " + priority); }
      }
      if (priority < 0 || priority > 9)
      {
         throw new MessageFormatException("Invalid message priority (" + priority + "). " +
                                          "Valid priorities are 0-9");
      }
      m.setJMSPriority(priority);

      if (this.isDisableMessageTimestamp())
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
         timeToLive = getTimeToLive();
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
         destination = (JBossDestination)getDestination();

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

         if (getDestination() != null &&
             !getDestination().equals(destination))
         {
            throw new UnsupportedOperationException("Where a default destination is specified " +
                                                    "for the sender and a destination is " +
                                                    "specified in the arguments to the send, " +
                                                    "these destinations must be equal");
         }
      }

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

      final boolean keepID = false;
      
      if (!keepID)
      {
         //Generate an id
         
         String id = UUID.randomUUID().toString();
         
         jbm.setJMSMessageID("ID:" + id);
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
      org.jboss.messaging.core.Destination coreDest =
         new DestinationImpl(dest.isQueue() ? DestinationType.QUEUE : DestinationType.TOPIC, dest.getName(), dest.isTemporary());
      
      //TODO - can optimise this copy to do copy lazily.
      org.jboss.messaging.core.Message messageToSend = jbm.getCoreMessage().copy();

      //FIXME - temp - for now we set destination as a header - should really be an attribute of the
      //send packet - along with scheduleddelivery time

      messageToSend.putHeader(org.jboss.messaging.core.Message.TEMP_DEST_HEADER_NAME, coreDest);

      // we now invoke the send(Message) method on the session, which will eventually be fielded
      // by connection endpoint
      session.send(messageToSend);
   }

   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      checkClosed();
      
      this.deliveryMode = deliveryMode;
   }

   public int getDeliveryMode() throws JMSException
   {
      checkClosed();
      
      return this.deliveryMode;
   }
  
   public boolean isDisableMessageID() throws JMSException
   {
      checkClosed();
      
      return this.disableMessageID;
   }

   public void setDisableMessageID(boolean value) throws JMSException
   {
      checkClosed();
      
      this.disableMessageID = value;   
   }

   public boolean isDisableMessageTimestamp() throws JMSException
   {      
      checkClosed();
      
      return this.disableMessageTimestamp;
   }

   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      checkClosed();
      
      this.disableMessageTimestamp = value;
   }

   public void setPriority(int priority) throws JMSException
   {
      checkClosed();
      
      this.priority = priority;
   }

   public int getPriority() throws JMSException
   {
      checkClosed();
      
      return this.priority;
   }

   public long getTimeToLive() throws JMSException
   {
      checkClosed();
      
      return this.timeToLive;
   }

   public void setTimeToLive(long timeToLive) throws JMSException
   {
      checkClosed();
      
      this.timeToLive = timeToLive;
   }

   // Public ---------------------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------
   
   // Package Private ------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void checkClosed() throws IllegalStateException
   {
      if (closed)
      {
         throw new IllegalStateException("Producer is closed");
      }
   }
   
   // Inner Classes --------------------------------------------------------------------------------

}
