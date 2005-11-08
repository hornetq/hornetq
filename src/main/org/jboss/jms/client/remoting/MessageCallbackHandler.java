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
package org.jboss.jms.client.remoting;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.jboss.aop.advice.Interceptor;
import org.jboss.jms.client.container.JMSMethodInvocation;
import org.jboss.jms.delegate.ConsumerDelegate;
import org.jboss.jms.delegate.SessionDelegate;
import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.util.JBossJMSException;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.callback.Callback;
import org.jboss.remoting.callback.HandleCallbackException;
import org.jboss.remoting.callback.InvokerCallbackHandler;
import org.jboss.remoting.transport.Connector;

import EDU.oswego.cs.dl.util.concurrent.SynchronousChannel;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox/a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageCallbackHandler implements InvokerCallbackHandler, Runnable
{
   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(MessageCallbackHandler.class);
   
   // Static --------------------------------------------------------
   
   public static void callOnMessage(ConsumerDelegate cons,
                                    SessionDelegate sess,
                                    MessageListener listener,
                                    String receiverID,
                                    boolean isConnectionConsumer,
                                    Message m)
         throws JMSException
   {
      preDeliver(sess, receiverID, m, isConnectionConsumer);
      
      try
      {      
         listener.onMessage(m);         
      }
      catch (RuntimeException e)
      {
         //See JMS1.1 spec 4.5.2
         int ackMode = sess.getAcknowledgeMode();
         
         if (ackMode == Session.AUTO_ACKNOWLEDGE || ackMode == Session.DUPS_OK_ACKNOWLEDGE)
         {
            //Cancel the message - this means it will be immediately redelivered

            cons.cancelMessage(m.getJMSMessageID());           
         }
         else
         {
            //Session is either transacted or CLIENT_ACKNOWLEDGE
            //We just deliver next message
         }           
      }
      
      postDeliver(sess, receiverID, m, isConnectionConsumer);     
   }
   
   protected static void preDeliver(SessionDelegate sess,
                                    String receiverID,
                                    Message m,
                                    boolean isConnectionConsumer)
      throws JMSException
   {
      //If this is the callback-handler for a connection consumer we don't want
      //to acknowledge or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         sess.preDeliver(m.getJMSMessageID(), receiverID);
      }         
   }
   
   protected static void postDeliver(SessionDelegate sess,
                                     String receiverID,
                                     Message m,
                                     boolean isConnectionConsumer)
      throws JMSException
   {
      //If this is the callback-handler for a connection consumer we don't want
      //to acknowledge or add anything to the tx for this session
      if (!isConnectionConsumer)
      {
         sess.postDeliver(m.getJMSMessageID(), receiverID);
      }         
   }
   
   // Attributes ----------------------------------------------------

   protected SynchronousChannel channel;   
   
   protected SessionDelegate sessionDelegate;
   
   protected ConsumerDelegate consumerDelegate;
   
   protected String receiverID;
   
   protected boolean isConnectionConsumer;
   
   protected boolean closed;
   
   protected volatile boolean listening;
   
   protected volatile boolean receiving;
   
   protected Thread listenerThread;
   
   protected Thread receiverThread;
   
   protected Object closedLock;
   
   protected MessageListener listener;
   
   protected int listenerThreadCount;
   
   protected volatile boolean stopping;
   
   //protected volatile boolean receiverStopping;
   
   protected volatile boolean waiting;

   // Constructors --------------------------------------------------

   public MessageCallbackHandler(boolean isCC)
   {
      channel = new SynchronousChannel();
      
      isConnectionConsumer = isCC;
      
      closedLock = new Object();
   }

   // InvokerCallbackHandler implementation -------------------------
   
   public void handleCallback(Callback callback) throws HandleCallbackException
   {
      if (log.isTraceEnabled()) { log.trace("receiving message " + callback.getParameter() + " from the remoting layer"); }

      synchronized (closedLock)
      {         
         Message m = (Message)callback.getParameter();
         
         if (closed)
         {
            log.error("Consumer is closed - ignoring message");
            //Note - we do not cancel the message if the handler is closed.
            //If the handler is closed then the corresponding serverconsumerdelegate
            //is either already closed or about to close, in which case it's deliveries
            //will be cancelled anyway.
         }
         
         try
         {                    
            //We attempt to put the message in the Channel
            //The call will return false if the message is not picked up the receiving or listening
            //thread in which case we need to cancel it
            
            boolean handled = false;
            while (waiting)
            {
               //channel.offer will *only* return true if there is a thread waiting to take a
               //message using take() or poll() hence we can guarantee there is no chance any
               //messages can arrive and are left in the channel without being handled -  this is
               //why we use a SynchronousChannel :)
               handled = channel.offer(m, 0);
               if (handled)
               {
                  break;
               }
            }
            
            if (!handled)
            {
               //There is no-one waiting for our message so we cancel it
               cancelMessage(m);
            }              
         }
         catch(InterruptedException e)
         {
            String msg = "Interrupted attempt to put message in the delivery buffer";
            log.error(msg);
            throw new HandleCallbackException(msg, e);
         }
      }
   }
   
   // Runnable implementation ---------------------------------------
   
   /**
    * Receive messages for the message listener.
    */
   public void run()
   {
      if (log.isTraceEnabled()) { log.trace("listener thread started"); }
      
      try
      {      
         while(true)
         {            
            if (log.isTraceEnabled()) { log.trace("blocking to take a message"); }
            
            if (!stopping)
            {               
               JBossMessage m = getMessage(0);
            
               callOnMessage(consumerDelegate, sessionDelegate, listener,
                             receiverID, isConnectionConsumer, m);
               
               if (log.isTraceEnabled()) { log.trace("message successfully handled by listener"); }  
               
            }
            else
            {
               break;
            }
         }     
      }
      catch(InterruptedException e)
      {
         log.debug("message listener thread interrupted, exiting");         
      }
      catch (JMSException e)
      {
         log.error("Failed to deliver message", e);
      }
      finally
      {
         listening = false;
      }
   }
   
   
   // Public --------------------------------------------------------
   

   
   public void setMessageListener(MessageListener listener) throws JMSException
   {
      //JMS consumer is single threaded, so it shouldn't be possible to
      //set a MessageListener while another thread is receiving
      
      if (receiving)
      {
         throw new JBossJMSException("Another thread is already receiving");
      }
      
      if (listening)
      {
         //Stop the current listener
         stopListener();
      }
      
      this.listener = listener;
      
      if (listener != null)
      {
         //Start the new listener
         listening = true;
         listenerThread = new Thread(this, "MessageListenerThread-" + listenerThreadCount++);
         listenerThread.start();
      }
      
   }
 
   
   public void close()
   {
      synchronized (closedLock)
      {
         closed = true;
            
         //Interrupt the listening thread if there is one
         if (listening)
         {
            stopListener();
         }
         
         if (receiving)
         {
            stopReceiver();
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
  
   }
   
 
   /**
    * Method used by the client thread to get a Message, if available.
    *
    * @param timeout - the timeout value in milliseconds. A zero timeount never expires, and the
    *        call blocks indefinitely. A -1 timeout means receiveNoWait(): return the next message
    *        or null if one is not immediately available. Returns null if the consumer is
    *        concurrently closed.
    */
   public Message receive(long timeout, JMSMethodInvocation jmsmi, Interceptor receiverInt)
      throws JMSException
   {            

      //Since the jms consumer is single threaded,
      //it shouldn't be possible for a receive to be called when another receive is in operation
      //or while a close is in operation.
      //But it is possible for a receive to be called while a message listener is set
      
      if (listening)
      {
         throw new JBossJMSException("A message listener is already registered");         
      }
      
      receiving = true;
      
      receiverThread = Thread.currentThread();
      
      long startTimestamp = System.currentTimeMillis();
      
      JBossMessage m = null;
      
      try
      {
         while(true)
         {
            try
            {
               if (timeout == 0)
               {
                  if (log.isTraceEnabled()) log.trace("receive with no timeout");
                  
                  if (!stopping)
                  {
                     m = getMessage(0);
                  }
                  
                  if (m == null)
                  {
                     return null;
                  }
                  
                  if (log.isTraceEnabled()) { log.trace("Got message: " + m); }
               }
               else if (timeout == -1)
               {
                  //ReceiveNoWait
                  if (log.isTraceEnabled()) { log.trace("receive noWait"); }                  
                  
                  if (!stopping)
                  {
                     m = getMessage(-1);
                  }
                  
                  if (m == null)
                  {
                     if (log.isTraceEnabled()) { log.trace("No message available"); }
                     return null;
                  }
               }
               else
               {
                  if (log.isTraceEnabled()) { log.trace("receive timeout " + timeout + " ms, blocking poll on queue"); }
                  
                  if (!stopping)
                  {
                     m = getMessage(timeout);
                  }
                  
                  if (m == null)
                  {
                     // timeout expired
                     if (log.isTraceEnabled()) { log.trace(timeout + " ms timeout expired"); }
                     
                     return null;
                  }
               }
            }
            catch(InterruptedException e)
            {
               if (log.isTraceEnabled()) { log.trace("Thread was interrupted"); }
               
               if (closed)
               {
                  return null;
               }
               else
               {
                  throw new JMSException("Interrupted");
               }
            }
            
            if (log.isTraceEnabled()) { log.trace("got " + m); }
                               
            if (!m.isExpired())
            {
               if (log.isTraceEnabled()) { log.trace("message " + m + " is not expired, returning it to the caller"); }
               
               preDeliver(sessionDelegate, receiverID, m, isConnectionConsumer);
               
               postDeliver(sessionDelegate, receiverID, m, isConnectionConsumer);
               
               return m;
            }
            
            log.debug("message expired, discarding");
            
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
         receiverThread = null;
      }
 
   }
   
   public void setSessionDelegate(SessionDelegate delegate)
   {
      this.sessionDelegate = delegate;
   }
   
   public void setConsumerDelegate(ConsumerDelegate delegate)
   {
      this.consumerDelegate = delegate;
   }
   
   public void setReceiverID(String receiverID)
   {
      this.receiverID = receiverID;
   }
   
   public synchronized MessageListener getMessageListener()
   {
      return listener;
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



   public String toString()
   {
      return "MessageCallbackHandler[" + receiverID + "]";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void cancelMessage(Message m)
   {
      try
      {
         consumerDelegate.cancelMessage(m.getJMSMessageID());
      }
      catch (Exception e)
      {
         //It may be ok that we cannot cancel the message - e.g.
         //if the corresponding server consumer delegate is already closed
         //in which case the deliveries will be cancelled anyway
         String msg = "Failed to cancel message";
         log.warn(msg);         
      }
   }
   
  
   
   protected void stopListener()
   {
      stopping = true;
      
      //The listener loop may not be waiting, so interrupting the thread will
      //not necessarily work, thus leaving it hanging
      //so we use the listenerStopping variable too
      listenerThread.interrupt();
      try
      {
         listenerThread.join(3000);
         if (listenerThread.isAlive())
         {
            listenerThread.interrupt();
            listenerThread.join();            
         }
         stopping = false;
      }
      catch (InterruptedException e)
      {
         log.error("Thread interrupted", e);
      }
            
   }
   
   protected void stopReceiver()
   {
      stopping = true;
      
      //The listener loop may not be waiting, so interrupting the thread will
      //not necessarily work, thus leaving it hanging
      //so we use the listenerStopping variable too
      receiverThread.interrupt();
      
      //FIXME - There is a possibility the receiver thread could still be waiting inside the receive() method
      //after the interrupt - need to deal with this
      
      stopping = false;
         
   }
   
   protected Message getMessageFromServer() throws JMSException
   {
      return consumerDelegate.getMessage();
   }
   
   protected void stopServerDelivery() throws JMSException
   {
      consumerDelegate.stopDelivering();
   }
   
   protected JBossMessage getMessage(long timeout) throws InterruptedException, JMSException
   {
      JBossMessage m = null;
      
      waiting = true;
      
      //Get message from server if one is available
      m = (JBossMessage)getMessageFromServer();
      
      
      if (m == null)
      {
         
         try
         {
            if (timeout == 0)
            {
               //Indefinite wait
               m = (JBossMessage)channel.take();         
            }
            else if (timeout == -1)
            {
               //Receive no-wait - do nothing               
            }
            else
            {
               //wait with timeout
               m = (JBossMessage)channel.poll(timeout);
            }
         }
         finally
         {
            //We only need to call this if we didn't receive a message synchronously
            waiting = false;
            if (!closed)
            {
               stopServerDelivery();
            }
         }
      }
      else
      {
         waiting = false;
      }
      
      if (m != null)
      {
         JBossMessage jm = (JBossMessage)m;  
         //if this is the handler for a connection consumer we don't want to set the session delegate
         //since this is only used for client acknowledgement which is illegal for a session
         //used for an MDB
         if (!this.isConnectionConsumer)
         {
            jm.setSessionDelegate(sessionDelegate);
         }
      }
      
      return m;
   }
   

   
   // Private -------------------------------------------------------
   
   
   // Inner classes -------------------------------------------------
}

