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
package org.jboss.jms.client;

import java.util.UUID;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

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
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JBossMessageProducer implements MessageProducer, QueueSender, TopicPublisher
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------      

   private static final Logger log = Logger.getLogger(JBossMessageProducer.class);
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();
   
   private ClientSession session;

   private Destination destination;

   private volatile boolean closed;
   
   private boolean disableMessageID = false;
   
   private boolean disableMessageTimestamp = false;
   
   private int priority = 4;
   
   private long timeToLive = 0;
   
   private int deliveryMode = DeliveryMode.PERSISTENT;
   

   // Constructors --------------------------------------------------
   
   public JBossMessageProducer(org.jboss.jms.client.api.ClientSession session, Destination destination)
   {
      this.session = session;
      this.destination = destination;
   }
   
   // MessageProducer implementation --------------------------------
   
   public void setDisableMessageID(boolean value) throws JMSException
   {
      checkClosed();
      log.warn("JBoss Messaging does not support disabling message ID generation");

      this.disableMessageID = value;
   }
   
   public boolean getDisableMessageID() throws JMSException
   {
      checkClosed();
      return disableMessageID;
   }
   
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      checkClosed();
      this.disableMessageTimestamp = value;
   }
   
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      checkClosed();
      return disableMessageTimestamp;
   }
   
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      checkClosed();
      this.deliveryMode = deliveryMode;
   }
   
   public int getDeliveryMode() throws JMSException
   {
      checkClosed();
      return deliveryMode;
   }
   
   public void setPriority(int defaultPriority) throws JMSException
   {
      checkClosed();
      
      if (defaultPriority < 0 || defaultPriority > 9)
      {
         throw new JMSException("Invalid message priority (" + priority + "). " +
                                          "Valid priorities are 0-9");
      }
      
      this.priority = defaultPriority;
   }
   
   public int getPriority() throws JMSException
   {
      checkClosed();
      return this.priority;
   }
   
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      checkClosed();
      this.timeToLive = timeToLive;
   }
   
   public long getTimeToLive() throws JMSException
   {
      checkClosed();
      return this.timeToLive;
   }
   
   public Destination getDestination() throws JMSException
   {
      checkClosed();
      return this.destination;
   }
   
   public void close() throws JMSException
   {
      closed = true;
   }
   
   public void send(Message message) throws JMSException
   {
      // by default the message never expires
      send(message, this.deliveryMode, this.priority, timeToLive);
   }
   
   /**
    * @param timeToLive - 0 means never expire.
    */
   public void send(Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException
   { 
      send(null, message, deliveryMode, priority, timeToLive);
   }
   
   public void send(Destination destination, Message message) throws JMSException
   {      
      send(destination, message, deliveryMode, priority, timeToLive);
   }

   public void send(Destination destination,
                    Message m,
                    int deliveryMode,
                    int priority,
                    long timeToLive) throws JMSException
   {
      if (destination != null && !(destination instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Not a JBossDestination:" + destination);
      }
      
      
      checkClosed();
      
      m.setJMSDeliveryMode(deliveryMode);

      if (priority < 0 || priority > 9)
      {
         throw new MessageFormatException("Invalid message priority (" + priority + "). " +
                                          "Valid priorities are 0-9");
      }
      m.setJMSPriority(priority);

      if (this.getDisableMessageTimestamp())
      {
         m.setJMSTimestamp(0l);
      }
      else
      {
         m.setJMSTimestamp(System.currentTimeMillis());
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
         jbm = convertMessage(destination, m);

         foreign = true;
      }
      else
      {
         jbm = (JBossMessage)m;
      }

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

      JBossDestination dest = (JBossDestination)destination;

      //Set the destination on the core message - TODO temp for refactoring
      org.jboss.messaging.core.Destination coreDest =
         new DestinationImpl(dest.isQueue() ? DestinationType.QUEUE : DestinationType.TOPIC, dest.getName(), dest.isTemporary());
      
      //TODO - can optimise this copy to do copy lazily.
      org.jboss.messaging.core.Message messageToSend = jbm.getCoreMessage().copy();

      //FIXME - temp - for now we set destination as a header - should really be an attribute of the
      //send packet - along with scheduleddelivery time

      // we now invoke the send(Message) method on the session, which will eventually be fielded
      // by connection endpoint
      session.send(messageToSend, coreDest);
   }

   // TopicPublisher Implementation ---------------------------------

   public Topic getTopic() throws JMSException
   {
      return (Topic)getDestination();
   }
   
   public void publish(Message message) throws JMSException
   {
      send(message);
   }
   
   public void publish(Topic topic, Message message) throws JMSException
   {
      send(topic, message);
   }
   
   public void publish(Message message, int deliveryMode, int priority, long timeToLive)
      throws JMSException
   {
      send(message, deliveryMode, priority, timeToLive);
   }
   
   public void publish(Topic topic, Message message, int deliveryMode,
                       int priority, long timeToLive) throws JMSException
   {
      send(topic, message, deliveryMode, priority, timeToLive);
   }

   // QueueSender Implementation ------------------------------------

   public void send(Queue queue, Message message) throws JMSException
   {
      send((Destination)queue, message);
   }
   
   public void send(Queue queue, Message message, int deliveryMode, int priority,
                    long timeToLive) throws JMSException
   {
      send((Destination)queue, message, deliveryMode, priority, timeToLive);
   }
   
   public Queue getQueue() throws JMSException
   {
      return (Queue)getDestination();
   }
   
   // Public --------------------------------------------------------

   public ClientSession getDelegate()
   {
      return session;
   }

   public String toString()
   {
      return "JBossMessageProducer->" + session;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------


   private void checkClosed() throws IllegalStateException
   {
      if (closed || session.isClosed())
      {
         throw new IllegalStateException("Producer is closed");
      }
   }

   private JBossMessage convertMessage(Destination destination, Message m)
         throws JMSException
   {
      JBossMessage jbm;
      if (m instanceof BytesMessage)
      {
         jbm = new JBossBytesMessage((BytesMessage) m);
      } 
      else if (m instanceof MapMessage)
      {
         jbm = new JBossMapMessage((MapMessage) m);
      } 
      else if (m instanceof ObjectMessage)
      {
         jbm = new JBossObjectMessage((ObjectMessage) m);
      } 
      else if (m instanceof StreamMessage)
      {
         jbm = new JBossStreamMessage((StreamMessage) m);
      } 
      else if (m instanceof TextMessage)
      {
         jbm = new JBossTextMessage((TextMessage) m);
      } 
      else
      {
         jbm = new JBossMessage(m);
      }

      //Set the destination on the original message
      m.setJMSDestination(destination);
      return jbm;
   }


   
   // Inner classes -------------------------------------------------
}
