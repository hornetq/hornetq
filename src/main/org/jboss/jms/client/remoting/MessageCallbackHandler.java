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

   public void handleCallback(InvocationRequest invocation) throws HandleCallbackException
   {
      try
      {
         Message m = (Message)invocation.getParameter();

         if (rv.put(m))
         {
            // TODO: supposedly my receiver thread got it. However I dont' have a hard guarantee
            return;
         }
         listener.onMessage(m);
      }
      catch(Throwable t)
      {
         throw new HandleCallbackException("Message not acknowledged");
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
    */
   public Message pullMessage(long timeout)
   {
      return (Message)rv.get(timeout);
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
