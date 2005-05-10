/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.remoting;

import org.jboss.remoting.InvokerCallbackHandler;
import org.jboss.remoting.InvocationRequest;
import org.jboss.remoting.HandleCallbackException;
import org.jboss.logging.Logger;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.jms.delegate.AcknowledgmentHandler;

import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.JMSException;

import EDU.oswego.cs.dl.util.concurrent.BoundedBuffer;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageCallbackHandler implements InvokerCallbackHandler, Runnable
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageCallbackHandler.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected AcknowledgmentHandler acknowledgmentHandler;

   private volatile boolean receiving;

   protected int capacity = 100;
   protected BoundedBuffer messages;

   protected MessageListener listener;
   protected Thread listenerThread;
   private int listenerThreadCount = 0;



   // Constructors --------------------------------------------------

   public MessageCallbackHandler()
   {
      messages = new BoundedBuffer(capacity);
   }

   // InvokerCallbackHandler implementation -------------------------

   public void handleCallback(InvocationRequest invocation) throws HandleCallbackException
   {

      Message m = (Message)invocation.getParameter();

      if (log.isTraceEnabled()) { log.trace("receiving from server: " + m); }

      try
      {
         messages.put(m);
         if (log.isTraceEnabled()) { log.trace("message " + m + " accepted for delivery"); }
      }
      catch(InterruptedException e)
      {
         String msg = "Interrupted attempt to put the message in the delivery buffer";
         log.warn(msg);
         throw new HandleCallbackException(msg, e);
      }
   }

   // Runnable implementation ---------------------------------------

   /**
    * Receive messages for the message listener.
    */
   public void run()
   {
      while(true)
      {
         try
         {
            Message m = (Message)messages.take();
            listener.onMessage(m);
            acknowledge(m);
         }
         catch(InterruptedException e)
         {
            log.warn("The message listener thread has been interrupted", e);
         }
         catch(Throwable t)
         {
            // TODO - different actions for client error/commuication error
            // TODO - try several times and then give up?
            log.error(t);
         }
      }
   }

   // Public --------------------------------------------------------

   public void setAcknowledgmentHandler(AcknowledgmentHandler acknowledgmentHandler)
   {
      this.acknowledgmentHandler = acknowledgmentHandler;
   }

   public synchronized MessageListener getMessageListener()
   {
      return listener;
   }

   public synchronized void setMessageListener(MessageListener listener)
   {
      if (this.listener == null && listener != null)
      {
         listenerThread = new Thread(this, "MessageListener Thread " + listenerThreadCount++);
      }
      this.listener = listener;
      listenerThread.start();
   }

   /**
    * Method used by the client thread to get a Message, if available.
    *
    * @param timeout - the timeout value in milliseconds. A zero timeount never expires, and the
    *        call blocks indefinitely.
    */
   public Message receive(long timeout) throws JMSException, InterruptedException
   {
      // make sure the current state allows receiving

      synchronized(this)
      {
         if (listener != null)
         {
            throw new JBossJMSException("A message listener is already registered");
         }
      }
      if (receiving)
      {
         throw new JBossJMSException("Another thread is already in receive.");
      }

      try
      {
         if (log.isTraceEnabled()) { log.trace("receive, timeout = "+timeout+" ms"); }

         receiving = true;
         Message m = null;
         if (timeout == 0)
         {
            m = (Message)messages.take();
         }
         else
         {
            m = (Message)messages.poll(timeout);
         }
         if (m != null)
         {
            acknowledge(m);
         }
         return m;
      }
      finally
      {
         receiving = false;
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   public void acknowledge(Message m) throws JMSException
   {
      acknowledgmentHandler.acknowledge(m.getJMSMessageID());
   }

   // Inner classes -------------------------------------------------
}




