/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;

import java.io.Serializable;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ServerConsumerDelegate implements ConsumerDelegate, Receiver
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerDelegate.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected String id;
   protected Distributor destination;
   protected ServerSessionDelegate parent;

   // Constructors --------------------------------------------------

   public ServerConsumerDelegate(String id, Distributor destination, ServerSessionDelegate parent)
   {
      this.id = id;
      this.destination = destination;
      this.parent = parent;

      // register myself with the destination
      destination.add(this);
   }

   // ConsumerDelegate implementation -------------------------------

   // Receiver implementation ---------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean handle(Routable r)
   {
      if (log.isTraceEnabled()) { log.trace("Receiving routable " + r); }
      return true;

   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
