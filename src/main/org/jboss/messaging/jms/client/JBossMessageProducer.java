/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.client;

import java.util.concurrent.atomic.AtomicLong;

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.IllegalStateException;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicPublisher;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.JBossDestination;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.UUIDGenerator;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
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
   
   private JBossConnection jbossConn;
   
   private final SimpleString connID;
   
   private ClientProducer producer;
   
   private boolean disableMessageID = false;
   
   private boolean disableMessageTimestamp = false;
   
   private int defaultPriority = 4;
   
   private long defaultTimeToLive = 0;
   
   private int defaultDeliveryMode = DeliveryMode.PERSISTENT;
   
   private JBossDestination defaultDestination;
   
   private final String messageIDPrefix;
   
   private final AtomicLong sequenceNumber = new AtomicLong(0);
   
   private ClientSession clientSession;

   // Constructors --------------------------------------------------
   
   public JBossMessageProducer(final JBossConnection jbossConn, final ClientProducer producer,
            final JBossDestination defaultDestination, final ClientSession clientSession) throws JMSException
   {
      this.jbossConn = jbossConn;
      
      this.connID = jbossConn.getUID();
      
      this.producer = producer;     
      
      this.defaultDestination = defaultDestination;

      this.clientSession = clientSession;
      
      //TODO the UUID should be generated at the JMS Connection level, 
      // then session, producers & messages ID could be created using simple sequences
      String uuid = UUIDGenerator.getInstance().generateSimpleStringUUID().toString();
      
      messageIDPrefix = "ID:" + uuid + ":";
   }
   
   // MessageProducer implementation --------------------------------
   
   public void setDisableMessageID(boolean value) throws JMSException
   {
      checkClosed();
      
      disableMessageID = value;
   }
   
   public boolean getDisableMessageID() throws JMSException
   {
      checkClosed();
      
      return disableMessageID;
   }
   
   public void setDisableMessageTimestamp(boolean value) throws JMSException
   {
      checkClosed();
      
      disableMessageTimestamp = value;
   }
   
   public boolean getDisableMessageTimestamp() throws JMSException
   {
      checkClosed();
      
      return disableMessageTimestamp;
   }
   
   public void setDeliveryMode(int deliveryMode) throws JMSException
   {
      checkClosed();
      
      this.defaultDeliveryMode = deliveryMode;
   }
   
   public int getDeliveryMode() throws JMSException
   {
      checkClosed();
      
      return this.defaultDeliveryMode;
   }
   
   public void setPriority(int defaultPriority) throws JMSException
   {
      checkClosed();
      
      this.defaultPriority = defaultPriority;
   }
   
   public int getPriority() throws JMSException
   {
      checkClosed();
      
      return defaultPriority;
   }
   
   public void setTimeToLive(long timeToLive) throws JMSException
   {
      checkClosed();
      
      this.defaultTimeToLive = timeToLive;
   }
   
   public long getTimeToLive() throws JMSException
   {
      checkClosed();
      
      return defaultTimeToLive;
   }
   
   public Destination getDestination() throws JMSException
   {
      checkClosed();
      
      return defaultDestination;
   }
   
   public void close() throws JMSException
   {
      try
      {
         producer.close();
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      }   
   }
   
   public void send(Message message) throws JMSException
   {
      checkClosed();

      message.setJMSDeliveryMode(defaultDeliveryMode);
      
      message.setJMSPriority(defaultPriority);
      
      doSend(message, defaultTimeToLive, null);
   }
   
   public void send(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
   { 
      checkClosed();

      message.setJMSDeliveryMode(deliveryMode);
      
      message.setJMSPriority(priority);
            
      doSend(message, timeToLive, null);
   }
   
   public void send(Destination destination, Message message) throws JMSException
   {      
      checkClosed();

      if (destination != null && !(destination instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Not a JBoss Destination:" + destination);
      }
      
      message.setJMSDeliveryMode(defaultDeliveryMode);
      
      message.setJMSPriority(defaultPriority);
      
      doSend(message, defaultTimeToLive, (JBossDestination)destination);
   }


   public void send(Destination destination,
                    Message message,
                    int deliveryMode,
                    int priority,
                    long timeToLive) throws JMSException
   {
      checkClosed();

      if (destination != null && !(destination instanceof JBossDestination))
      {
         throw new InvalidDestinationException("Not a JBoss Destination:" + destination);
      }

      message.setJMSDeliveryMode(deliveryMode);
      
      message.setJMSPriority(priority);
            
      doSend(message, timeToLive, (JBossDestination)destination);
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

   public String toString()
   {
      return "JBossMessageProducer->" + producer;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private void doSend(Message message, long timeToLive, JBossDestination destination) throws JMSException
   {
      if (timeToLive == 0)
      {
         message.setJMSExpiration(0);
      }
      else
      {
         message.setJMSExpiration(System.currentTimeMillis() + timeToLive);
      }
      
      if (!disableMessageTimestamp)
      {
         message.setJMSTimestamp(System.currentTimeMillis());
      }
      else
      {
         message.setJMSTimestamp(0);
      }
      
      SimpleString address = null;
      
      if (destination == null)
      {
      	if (defaultDestination == null)
      	{
      		throw new InvalidDestinationException("Destination must be specified on send with an anonymous producer");
      	}
      	
      	destination = defaultDestination;
      }
      else
      {
      	if (defaultDestination != null)
      	{
      		if (!destination.equals(defaultDestination))
      		{
      			throw new JMSException("Where a default destination is specified " +
                     "for the sender and a destination is " +
                     "specified in the arguments to the send, " +
                     "these destinations must be equal");
      		}
      	}
      	
      	address = destination.getSimpleAddress();
      }
      
      JBossMessage jbm;

      boolean foreign = false;

      // First convert from foreign message if appropriate
      if (!(message instanceof JBossMessage))
      {
         // JMS 1.1 Sect. 3.11.4: A provider must be prepared to accept, from a client,
         // a message whose implementation is not one of its own.

         if (message instanceof BytesMessage)
         {
            jbm = new JBossBytesMessage((BytesMessage)message, clientSession );
         }
         else if (message instanceof MapMessage)
         {
            jbm = new JBossMapMessage((MapMessage)message, clientSession);
         }
         else if (message instanceof ObjectMessage)
         {
            jbm = new JBossObjectMessage((ObjectMessage)message, clientSession);
         }
         else if (message instanceof StreamMessage)
         {
            jbm = new JBossStreamMessage((StreamMessage)message, clientSession);
         }
         else if (message instanceof TextMessage)
         {
            jbm = new JBossTextMessage((TextMessage)message, clientSession);
         }
         else
         {
            jbm = new JBossMessage(message, clientSession);
         }

         // Set the destination on the original message
         message.setJMSDestination(destination);

         foreign = true;
      }
      else
      {
         jbm = (JBossMessage)message;
      }

      if (!disableMessageID)
      {
         // Generate an id
         jbm.setJMSMessageID(messageIDPrefix + sequenceNumber.incrementAndGet());
      }

      if (foreign)
      {
         message.setJMSMessageID(jbm.getJMSMessageID());
      }

      jbm.setJMSDestination(destination);

      try
      {
         jbm.doBeforeSend();
      }
      catch (Exception e)
      {
         JMSException je = new JMSException(e.getMessage());
         
         je.initCause(e);
         
         throw je;
      }

      ClientMessage coreMessage = jbm.getCoreMessage();

      if (jbossConn.hasNoLocal())
      {
         coreMessage.putStringProperty(JBossConnection.CONNECTION_ID_PROPERTY_NAME, connID);
      }

      try
      {      	
      	producer.send(address, coreMessage);         
      }
      catch (MessagingException e)
      {
         throw JMSExceptionHelper.convertFromMessagingException(e);     
      } 
   }
   
   private void checkClosed() throws JMSException
   {
      if (producer.isClosed())
      {
         throw new IllegalStateException("Producer is closed");
      }
   }
   
   // Inner classes -------------------------------------------------
}
