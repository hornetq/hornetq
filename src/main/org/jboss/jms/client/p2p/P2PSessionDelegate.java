/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.p2p;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.transaction.xa.XAResource;

import org.jboss.jms.BytesMessageImpl;
import org.jboss.jms.MapMessageImpl;
import org.jboss.jms.MessageImpl;
import org.jboss.jms.ObjectMessageImpl;
import org.jboss.jms.StreamMessageImpl;
import org.jboss.jms.TextMessageImpl;
import org.jboss.jms.client.BrowserDelegate;
import org.jboss.jms.client.ConsumerDelegate;
import org.jboss.jms.client.ProducerDelegate;
import org.jboss.jms.client.SessionDelegate;

/**
 * The p2p session
 * 
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public class P2PSessionDelegate
   implements SessionDelegate
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private P2PConnectionDelegate connection = null;
   private int acknowledgeMode;
   private boolean closed = false; // TOD: make sure this is the default.
   private MessageListener messageListener = null;
   private boolean transacted = false;
   // TOD: Might be able to eliminate the seperate lists by implementing a
   //interface which just does a close() if that is all we're uisng this for...
   private List messageConsumers = new ArrayList();
   private List messageProducers = new ArrayList();
   private List queueBrowsers = new ArrayList();

   private Map unacknowledgedMessageMap = new TreeMap();
   private long nextDeliveryId = 0;
   private boolean recovering = false;
   private Object recoveryLock = new Object();
   private List uncommittedMessages = new ArrayList();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public P2PSessionDelegate(P2PConnectionDelegate connection, boolean transaction, int acknowledgeMode)
      throws JMSException
   {
      this.connection = connection;
      this.transacted = transaction;
      this.acknowledgeMode = acknowledgeMode;
   }

   // Public --------------------------------------------------------

   // SessionDelegate implementation -----------------------------

   public void close() throws JMSException
   {
      if (!this.closed)
      {
          if (this.transacted)
          {
              this.rollback();
          }
          Iterator iterator = this.messageConsumers.iterator();
          while (iterator.hasNext())
          {
              ((ConsumerDelegate) iterator.next()).close();
              iterator.remove();
          }
          iterator = this.messageProducers.iterator();
          while (iterator.hasNext())
          {
              ((ProducerDelegate) iterator.next()).close();
          }
          iterator = this.queueBrowsers.iterator();
          while (iterator.hasNext())
          {
              ((BrowserDelegate) iterator.next()).close();
          }
          this.closed = true;
      }
   }

   public void closing() throws JMSException
   {
   }

   public void commit() throws JMSException
   {
      this.throwExceptionIfClosed();
      if (this.transacted)
      {
          this.recovering = true;
          if (this.uncommittedMessages.size() > 0)
          {
              this.connection.send((Collection) ((ArrayList) this.uncommittedMessages).clone());
          }
          this.unacknowledgedMessageMap.clear();
          this.uncommittedMessages.clear();
          this.recovering = false;
          synchronized (this.recoveryLock)
          {
              this.recoveryLock.notify();
          }
      }
      else
      {
          throw new IllegalStateException("Illegal Operation: This is not a transacted Session.");
      }
   }

   public BrowserDelegate createBrowser(Queue queue, String selector) throws JMSException
   {
      this.throwExceptionIfClosed();
      return new P2PBrowserDelegate(this, queue, selector);
   }

   public BytesMessage createBytesMessage() throws JMSException
   {
      this.throwExceptionIfClosed();
      return new BytesMessageImpl();
   }

   public ConsumerDelegate createConsumer(
      Destination destination,
      String subscription,
      String selector,
      boolean noLocal)
      throws JMSException
   {
      this.throwExceptionIfClosed();
      ConsumerDelegate messageConsumer = new P2PConsumerDelegate(this, destination, selector, noLocal);
      this.messageConsumers.add(messageConsumer);
      return messageConsumer;
   }

   public MapMessage createMapMessage() throws JMSException
   {
      this.throwExceptionIfClosed();
      return new MapMessageImpl();
   }

   public javax.jms.Message createMessage() throws JMSException
   {
      this.throwExceptionIfClosed();
      return new MessageImpl();
   }

   public ObjectMessage createObjectMessage(Serializable object) throws JMSException
   {
      this.throwExceptionIfClosed();
      return new ObjectMessageImpl(object);
   }

   public ProducerDelegate createProducer(Destination destination) throws JMSException
   {
      this.throwExceptionIfClosed();
      ProducerDelegate messageProducer = new P2PProducerDelegate(this, destination);
      this.messageProducers.add(messageProducer);
      return messageProducer;
   }

   public StreamMessage createStreamMessage() throws JMSException
   {
      this.throwExceptionIfClosed();
      return new StreamMessageImpl();
   }

   public Destination createTempDestination(int type) throws JMSException
   {
      // TODO createTempDestination
      return null;
   }

   public TextMessage createTextMessage(String text) throws JMSException
   {
      this.throwExceptionIfClosed();
      return new TextMessageImpl(text);
   }

   public Destination getDestination(String name) throws JMSException
   {
      // TODO getDestination
      return null;
   }

   public XAResource getXAResource()
   {
      // TODO getXAResource
      return null;
   }

   public void recover() throws JMSException
   {
      this.throwExceptionIfClosed();
      if (this.transacted)
      {
          throw new IllegalStateException("Illegal Operation: This is a transacted Session.  Use rollback instead.");
      }
      synchronized (this.unacknowledgedMessageMap)
      {
          this.recovering = true;
          Map clone = (Map) ((TreeMap) this.unacknowledgedMessageMap).clone();
          this.unacknowledgedMessageMap.clear();
          this.restart(clone);
      }
   }

   public void rollback() throws JMSException
   {
      this.throwExceptionIfClosed();
      if (this.transacted)
      {
          synchronized (this.unacknowledgedMessageMap)
          {
              this.recovering = true;
              Map clone = (Map) ((TreeMap) this.unacknowledgedMessageMap).clone();
              this.unacknowledgedMessageMap.clear();
              this.restart(clone);
          }
          this.uncommittedMessages.clear();
      }
      else
      {
          throw new IllegalStateException("Illegal Operation: This is not a transacted Session.");
      }
   }

   public void run()
   {
      // TODO run
   }

   public void setMessageListener(MessageListener listener) throws JMSException
   {
      this.throwExceptionIfClosed();
      this.messageListener = listener;
   }

   public void unsubscribe(String name) throws JMSException
   {
      this.throwExceptionIfClosed();
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   synchronized void send(MessageImpl message) throws JMSException
   {
       if (this.transacted)
       {
           this.uncommittedMessages.add(message.clone());
       }
       else
       {
           this.connection.send(message);
       }
   }

   public void acknowledge(Message message, boolean acknowledge)
   {
       if (!this.transacted)
       {
           synchronized (this.unacknowledgedMessageMap)
           {
               Iterator iterator = this.unacknowledgedMessageMap.keySet().iterator();
               while (iterator.hasNext())
               {
                   Long currentKey = (Long) iterator.next();
                   if (currentKey.longValue() <= ((MessageImpl) message).deliveryId)
                   {
                       iterator.remove();
                   }
               }
           }
       }
   }
   void deliver(MessageImpl message)
   {
       this.deliver(message, false);
   }

   private void deliver(MessageImpl message, boolean recoveryOperation)
   {
       if (this.recovering && !recoveryOperation)
       {
           synchronized (this.recoveryLock)
           {
               try
               {
                   this.recoveryLock.wait();
               }
               catch (InterruptedException e)
               {
               }
           }
       }
       message.setSession(this);
       message.setDeliveryId(++this.nextDeliveryId);
       Iterator iterator = this.messageConsumers.iterator();
       if (this.acknowledgeMode != Session.AUTO_ACKNOWLEDGE)
       {
           synchronized (unacknowledgedMessageMap)
           {
               this.unacknowledgedMessageMap.put(new Long(this.nextDeliveryId), message);
           }
       }
       while (iterator.hasNext())
       {
           ((P2PConsumerDelegate) iterator.next()).deliver(message);
       }
   }
   // Private --------------------------------------------------------

   private void throwExceptionIfClosed() throws IllegalStateException
   {
       if (this.closed)
       {
           throw new IllegalStateException("The session is closed.");
       }
   }

   private void restart(final Map unacknowledgedMessage)
   {
       Thread thread = new Thread(new Runnable()
       {
           public void run()
           {
               Iterator iterator = unacknowledgedMessage.keySet().iterator();
               while (iterator.hasNext())
               {
                   MessageImpl message = (MessageImpl) unacknowledgedMessage.get(iterator.next());
                   message.setJMSRedelivered(true);
                   deliver(message, true);
               }
               recovering = false;
               synchronized (recoveryLock)
               {
                   recoveryLock.notify();
               }
           }
       });
       thread.start();
   }

   // Inner Classes --------------------------------------------------

}
