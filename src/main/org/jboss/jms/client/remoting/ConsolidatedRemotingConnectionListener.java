/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.remoting;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.jboss.jms.client.container.ConnectionFailureListener;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.logging.Logger;
import org.jboss.remoting.Client;
import org.jboss.remoting.ConnectionListener;

/**
 * The ONLY remoting connection listener for a JMS connection's underlying remoting connection.
 * Added to the remoting connection when the JMS connection is created, and removed when the
 * JMS connection is closed. Any second tier listeners (the JMS connection ExceptionListener,
 * and the HA's connection failure detector) are registered with this consolidated listener and not
 * with the remoting connection directly.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConsolidatedRemotingConnectionListener implements ConnectionListener
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ConsolidatedRemotingConnectionListener.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private ConnectionState state;

   private ExceptionListener jmsExceptionListener;

   private ConnectionFailureListener remotingListener;

   // Constructors ---------------------------------------------------------------------------------

   public ConsolidatedRemotingConnectionListener()
   {
   }

   // ConnectionListener implementation ------------------------------------------------------------

   public void handleConnectionException(Throwable throwable, Client client)
   {
      // forward the exception to delegate listener and JMS ExceptionListeners; synchronize
      // to avoid race conditions

      ExceptionListener jmsExceptionListenerCopy;
  
      ConnectionFailureListener remotingListenerCopy;

      synchronized(this)
      {
         jmsExceptionListenerCopy = jmsExceptionListener;

         remotingListenerCopy = remotingListener;
      }
      
      boolean forwardToJMSListener = true;

      if (remotingListenerCopy != null)
      {
         try
         {
            log.debug(this + " forwarding remoting failure \"" + throwable + "\" to " + remotingListenerCopy);
            
            //We only forward to the JMS listener if failover did not successfully handle the exception
            //If failover handled the exception transparently then there is effectively no problem
            //with the logical connection that the client needs to be aware of
            forwardToJMSListener = !remotingListenerCopy.handleConnectionException(throwable, client);
         }
         catch(Exception e)
         {
            log.warn("Failed to forward " + throwable + " to " + remotingListenerCopy, e);
         }
      }
      
      if (forwardToJMSListener && jmsExceptionListenerCopy != null)
      {
         JMSException jmsException = null;

         if (throwable instanceof Error)
         {
            final String msg = "Caught Error on underlying remoting connection";
            log.error(this + ": " + msg, throwable);
            jmsException = new JMSException(msg + ": " + throwable.getMessage());
         }
         else if (throwable instanceof Exception)
         {
            Exception e = (Exception)throwable;
            jmsException = new JMSException("Failure on underlying remoting connection");
            jmsException.setLinkedException(e);
         }
         else
         {
            // Some other Throwable subclass
            final String msg = "Caught Throwable on underlying remoting connection";
            log.error(this + ": " + msg, throwable);
            jmsException = new JMSException(msg + ": " + throwable.getMessage());
         }

         jmsExceptionListenerCopy.onException(jmsException);
      }
   }

   // Public ---------------------------------------------------------------------------------------

   public synchronized void setDelegateListener(ConnectionFailureListener l)
   {
      log.debug(this + " setting delegate listener " + l);
      
      if (remotingListener != null)
      {
         throw new IllegalStateException("There is already a connection listener for the connection");
      }
      
      remotingListener = l;
   }

   public synchronized void addJMSExceptionListener(ExceptionListener jmsExceptionListener)
   {
      log.debug(this + " adding JMS exception listener " + jmsExceptionListener);
      this.jmsExceptionListener = jmsExceptionListener;
   }

   public synchronized ExceptionListener getJMSExceptionListener()
   {
      return jmsExceptionListener;
   }

   /**
    * Clears all delegate listeners
    */
   public synchronized void clear()
   {
      jmsExceptionListener = null;
      remotingListener = null;
      log.debug(this + " cleared");
   }

   public void setConnectionState(ConnectionState state)
   {
      this.state = state;
   }

   public String toString()
   {
      if (state == null)
      {
         return "ConsolidatedListener(UNINITIALIZED)";
      }
      return state + ".ConsolidatedListener";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
