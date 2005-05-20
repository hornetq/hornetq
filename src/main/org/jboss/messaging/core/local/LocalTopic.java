/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;


import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Acknowledgment;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.Set;
import java.util.Iterator;

/**
 * A LocalTopic implements a Publishes/Subscriber messaging domain. It sends a message to all
 * receivers connected at the time the message is sent. All receivers are in the same address space.
 * By default a topic is configured as an asynchronous Channel.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalTopic extends AbstractDestination
{
   private static final Logger log = Logger.getLogger(LocalTopic.class);

   // Constructors --------------------------------------------------

   public LocalTopic(Serializable id)
   {
       super(id, false);
   }

   // Channel implementation ----------------------------------------

   public boolean handle(Routable r)
   {
      if (log.isTraceEnabled()) { log.trace(this + " forwarding to router"); }

      boolean synchronous = isSynchronous();

      Set acks = router.handle(r);

      boolean allSuccessful = false;
      if (!acks.isEmpty())
      {
         allSuccessful = true;
         for(Iterator i = acks.iterator(); i.hasNext() && allSuccessful; )
         {
            allSuccessful &= ((Acknowledgment)i.next()).isPositive();
         }
      }

      if (acks.isEmpty() && !synchronous || allSuccessful)
      {
         // I do not store undeliverable messages, but I positively ACK them
         if (log.isTraceEnabled()) { log.trace(this + " successful synchronous delivery"); }
         return true;
      }

      return updateAcknowledgments(r, acks);
   }

   public boolean isStoringUndeliverableMessages()
   {
      // positively acknowledge and discard a message if there are no receivers
      return false;
   }

   // AbstractDestination implementation ----------------------------

   protected AbstractRouter createRouter()
   {
       return new PointToMultipointRouter("P2MPRouter");
   }

}
