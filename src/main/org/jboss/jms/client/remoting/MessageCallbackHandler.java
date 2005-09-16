/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.remoting;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;
import org.jboss.remoting.transport.Connector;

import EDU.oswego.cs.dl.util.concurrent.BoundedBuffer;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageCallbackHandler implements InvokerCallbackHandler, Runnable
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(MessageCallbackHandler.class);
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected int capacity = 100;
   protected BoundedBuffer messages;
   protected SessionDelegate sessionDelegate;
   protected String receiverID;
   
   private volatile boolean receiving;
   private Thread receivingThread;
   
   
   protected MessageListener listener;
   protected Thread listenerThread;
   private int listenerThreadCount = 0;
   
   private volatile boolean closed;
   
   
   
   // Constructors --------------------------------------------------

   public MessageCallbackHandler()
   {
      this.messages = new BoundedBuffer(capacity);	
   }

   // InvokerCallbackHandler implementation -------------------------
   
   public void handleCallback(Callback callback) throws HandleCallbackException
   {
      if (log.isTraceEnabled()) { log.trace("receiving message " + callback.getParameter() + " from the remoting layer"); }

      Message m = (Message)callback.getParameter();
      
      try
      {			
         JBossMessage jm = (JBossMessage)m;	
         jm.setSessionDelegate(sessionDelegate);
         messages.put(m);
         if (log.isTraceEnabled()) { log.trace("message " + m + " queued in the client delivery queue"); }
      }
      catch(InterruptedException e)
      {
         String msg = "Interrupted attempt to put message in the delivery buffer";
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
      if (log.isTraceEnabled()) { log.trace("listener thread started"); }
      
      while(true)
      {
         try
         {
            if (log.isTraceEnabled()) { log.trace("blocking to take a message"); }
            JBossMessage m = (JBossMessage)messages.take();
            preDeliver(m);   
            listener.onMessage(m);
            if (log.isTraceEnabled()) { log.trace("message successfully handled by listener"); }
            postDeliver(m);
         }
         catch(InterruptedException e)
         {
            log.debug("message listener thread interrupted, exiting");
            return;
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
   
   
   public void setSessionDelegate(SessionDelegate delegate)
   {
      this.sessionDelegate = delegate;
   }
   
   public void setReceiverID(String receiverID)
   {
      this.receiverID = receiverID;
   }
   
   
   
   public synchronized MessageListener getMessageListener()
   {
      return listener;
   }
   
   public synchronized void setMessageListener(MessageListener listener) throws JMSException
   {
      if (receiving)
      {
         throw new JBossJMSException("Another thread is already receiving");
      }
      
      if (listenerThread != null)
      {
         listenerThread.interrupt();
      }
      
      this.listener = listener;
      if (listener != null)
      {
         listenerThread = new Thread(this, "MessageListenerThread-" + listenerThreadCount++);
         listenerThread.start();
      }
   }

   /**
    * TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
    */
   private Connector callbackServer;
   // I keep the client reference since I need to use the same client to remove a listener (sessionID)
   private Client client;
   public void setCallbackServer(Connector callbackServer, Client client)
   {
      this.callbackServer = callbackServer;
      this.client = client;
   }


   /**
    * Method used by the client thread to get a Message, if available.
    *
    * @param timeout - the timeout value in milliseconds. A zero timeount never expires, and the
    *        call blocks indefinitely. A -1 timeout means receiveNoWait(): return the next message
    *        or null if one is not immediately available. Returns null if the consumer is
    *        concurrently closed.
    */
   public Message receive(long timeout) throws JMSException, InterruptedException
   {
      
      long startTimestamp = System.currentTimeMillis();
      
      // make sure the current state allows receiving
      
      synchronized(this)
      {
         if (listener != null)
         {
            throw new JBossJMSException("A message listener is already registered");
         }
         if (receiving)
         {
            throw new JBossJMSException("Another thread is already receiving");
         }
         receiving = true;
      }
      
      try
      {
         receivingThread = Thread.currentThread();
         
         JBossMessage m = null;
         while(true)
         {
            try
            {
               if (timeout == 0)
               {
                  if (log.isTraceEnabled()) log.trace("receive with no timeout");
                    
                  m = ((JBossMessage)messages.take());

                  if (log.isTraceEnabled()) { log.trace("Got message: " + m); }
               }
               else if (timeout == -1)
               {
                  //ReceiveNoWait
                  if (log.isTraceEnabled()) { log.trace("receive noWait"); }

                  m = ((JBossMessage)messages.poll(0));
                 
                  if (m == null)
                  {
                     if (log.isTraceEnabled()) { log.trace("No message available"); }
                     return null;
                  }
               }
               else
               {
                  if (log.isTraceEnabled()) { log.trace("receive timeout " + timeout + " ms"); }

                  m = ((JBossMessage)messages.poll(timeout));
                                    
                  if (m == null)
                  {
                     // timeout expired
                     if (log.isTraceEnabled()) { log.trace("Timeout expired"); }
                     return null;
                  }
               }
            }
            catch(InterruptedException e)
            {
               if (log.isTraceEnabled()) { log.trace("Thread was interrupted"); }
               if (closed)
               {
                  if (log.isTraceEnabled()) { log.trace("It's closed"); }
                  return null;
               }
               else
               {
                  throw e;
               }
            }
            
            if (log.isTraceEnabled()) { log.trace("Calling delivered()"); }
            
            // notify that the message has been delivered (not necessarily acknowledged though)

            receivingThread = null; // Crucial - in case thread is interrupted during pre or postDeliver
            preDeliver(m);
            postDeliver(m);
            
            if (!m.isExpired())
            {
               if (log.isTraceEnabled()) { log.trace("Message is not expired, returning it to the caller"); }
               return m;
            }

            log.debug("Message expired");

            // discard the message, adjust timeout and reenter the buffer
            if (timeout != 0)
            {
               timeout -= System.currentTimeMillis() - startTimestamp;
            }
         }
      }
      finally
      {
         receiving = false;
         receivingThread = null;
      }
   }
   
   public void close()
   {
      if (closed)
      {
         return;
      }
      closed = true;
      
      if (log.isTraceEnabled()) { log.trace("Closing:" + this); }
      
      if (receivingThread != null)
      {
         if (log.isTraceEnabled()) { log.trace("Interrupting receiving thread:"); }
         receivingThread.interrupt();
      }

      // TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
      try
      {
         // unregister this callback handler and stop the callback server

         client.removeListener(this);
         log.debug("Listener removed from server");

         callbackServer.stop();
         log.debug("Closed callback server " + callbackServer.getInvokerLocator());
      }
      catch(Throwable e)
      {
         log.warn("Failed to clean up callback handler/callback server", e);
      }
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   public void preDeliver(Message m) throws JMSException
   {
      sessionDelegate.preDeliver(m.getJMSMessageID(), receiverID);
   }
   
   public void postDeliver(Message m) throws JMSException
   {
      sessionDelegate.postDeliver(m.getJMSMessageID(), receiverID);
   }
   
   // Inner classes -------------------------------------------------
}




