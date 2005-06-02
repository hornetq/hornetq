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
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossMessage;

import javax.jms.Destination;
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

   //protected AcknowledgmentHandler acknowledgmentHandler;


   protected int capacity = 100;
   protected BoundedBuffer messages;
	protected SessionDelegate sessionDelegate;
	protected String receiverID;
	protected Destination destination;

   private volatile boolean receiving;
   private Thread receivingThread;


   protected MessageListener listener;
   protected Thread listenerThread;
   private int listenerThreadCount = 0;

   private volatile boolean closed;



   // Constructors --------------------------------------------------

   public MessageCallbackHandler(Destination destination)
   {
      this.messages = new BoundedBuffer(capacity);	
		this.destination = destination;
   }

   // InvokerCallbackHandler implementation -------------------------

   public void handleCallback(InvocationRequest invocation) throws HandleCallbackException
   {

      Message m = (Message)invocation.getParameter();

      if (log.isTraceEnabled()) { log.trace("receiving from server: " + m); }

      try
      {			
			JBossMessage jm = (JBossMessage)m;	
			jm.setSessionDelegate(sessionDelegate);
         messages.put(m);
         if (log.isTraceEnabled()) { log.trace("message " + m + " accepted for delivery"); }
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
            Message m = (Message)messages.take();				
            listener.onMessage(m);
            if (log.isTraceEnabled()) { log.trace("message successfully handled by listener"); }
            delivered(m);
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
    * Method used by the client thread to get a Message, if available.
    *
    * @param timeout - the timeout value in milliseconds. A zero timeount never expires, and the
    *        call blocks indefinitely. Returns null if the consumer is concurrently closed.
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
         if (log.isTraceEnabled()) { log.trace("receive, timeout = " + timeout + " ms"); }

         receivingThread = Thread.currentThread();

         JBossMessage m = null;
         while(true)
         {
            try
            {
               if (timeout <= 0)
               {
						if (log.isTraceEnabled()) log.trace("receive with no timeout");
                  m = (JBossMessage)messages.take();
						if (log.isTraceEnabled()) log.trace("Got message:" + m);
               }
               else
               {
                  m = (JBossMessage)messages.poll(timeout);

                  if (m == null)
                  {
                     // receive expired
                     return null;
                  }
               }
            }
            catch(InterruptedException e)
            {
               if (closed)
               {
                  return null;
               }
               else
               {
                  throw e;
               }
            }

				if (log.isTraceEnabled()) log.trace("Calling delivered()");
				
            //Notify that the message has been delivered (not necessarily acknowledged though)
				delivered(m);
				
				if (log.isTraceEnabled()) log.trace("Called delivered()");

            if (!m.isExpired())
            {
					if (log.isTraceEnabled()) log.trace("Message is not expired");
               return m;
            }

            // discard the message, adjust timeout and reenter the buffer
            timeout -= System.currentTimeMillis() - startTimestamp;
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
      if (receivingThread != null)
      {
         receivingThread.interrupt();
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   public void delivered(Message m) throws JMSException
   {
      sessionDelegate.delivered(m.getJMSMessageID(), destination, receiverID);
   }

   // Inner classes -------------------------------------------------
}




