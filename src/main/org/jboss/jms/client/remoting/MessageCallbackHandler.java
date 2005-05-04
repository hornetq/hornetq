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
import org.jboss.remoting.Client;
import org.jboss.logging.Logger;
import org.jboss.jms.util.RendezVous;

import javax.jms.MessageListener;
import javax.jms.Message;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageCallbackHandler implements InvokerCallbackHandler
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(MessageCallbackHandler.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   /** the remoting client this callback handler is registered with **/
   protected Client client;
   protected MessageListener listener;
   protected RendezVous rv;

   // Constructors --------------------------------------------------

   public MessageCallbackHandler(Client client)
   {
      this.client = client;
      rv = new RendezVous();
   }

   // InvokerCallbackHandler implementation -------------------------

   /**
    * The method first tries to aquire the handler's lock, so in order to accept asynchronous
    * deliveries, the handler must be unlocked.
    */
   public void handleCallback(InvocationRequest invocation) throws HandleCallbackException
   {
      synchronized(rv)
      {
         try
         {
            Message m = (Message)invocation.getParameter();

            if (log.isTraceEnabled()) { log.trace("receiving asyncronous message " + m); }


            if (rv.put(m))
            {
               // TODO: supposedly my receiver thread got it. However I dont' have a hard guarantee
               if (log.isTraceEnabled()) { log.trace("message " + m + " accepted"); }
               return;
            }

            if (listener == null)
            {
               if (log.isTraceEnabled()) { log.trace("no one to handle message " + m.getJMSMessageID() + ", nacking ..."); }
               throw new NACKCallbackException();
            }

            listener.onMessage(m);
            if (log.isTraceEnabled()) { log.trace("message " + m.getJMSMessageID() + " submitted to listener"); }
         }
         catch(NACKCallbackException e)
         {
            throw e;
         }
         catch(Throwable t)
         {
            throw new HandleCallbackException("Failed to handle the message", t);
         }
      }
   }

   // Public --------------------------------------------------------

   public Client getClient()
   {
      return client;
   }

   public MessageListener getMessageListener()
   {
      return listener;
   }

   public void setMessageListener(MessageListener listener)
   {
      this.listener = listener;
   }

   /**
    * Method used by the client thread to get a Message, if available.
    *
    * @param timeout - the timeout value in milliseconds. A zero timeount never expires, and the
    *        call blocks indefinitely.
    */
   public Message pullMessage(long timeout)
   {
      return (Message)rv.get(timeout);
   }

   /**
    * Returns a reference to the MessageCallbackHandler's rendezVous. Useful for its synchronization
    * lock.
    */
   public Object getRendezVous()
   {
      return rv;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
