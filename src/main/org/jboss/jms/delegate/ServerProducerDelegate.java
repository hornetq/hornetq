/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.logging.Logger;

import javax.jms.Message;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerProducerDelegate implements ProducerDelegate
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerProducerDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   protected Receiver destination;
   protected ServerSessionDelegate parent;

   // Constructors --------------------------------------------------

   public ServerProducerDelegate(String id, Receiver destination, ServerSessionDelegate parent)
   {
      this.id = id;
      this.destination = destination;
      this.parent = parent;
   }

   // ProducerDelegate implementation ------------------------

   public void send(Message m)
   {
      if (log.isTraceEnabled()) { log.trace("sending message " + m + " to the core"); }

      try
      {
         boolean acked = destination.handle((Routable)m);

         if (!acked)
         {
            log.debug("The message was not acknowledged");
         }
      }
      catch(Throwable t)
      {
         log.error("Message handling failure", t);
      }

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
