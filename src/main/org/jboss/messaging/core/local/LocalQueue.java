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

/**
 * A LocalQueue implements a Point to Point messaging domain. It sends a message to one and only one
 * receiver. All receivers are in the same address space. By default a queue is configured as an
 * asynchronous Channel.
 *
 * @see org.jboss.messaging.core.Channel

 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalQueue extends AbstractDestination
{

   private static final Logger log = Logger.getLogger(LocalQueue.class);

   // Constructors --------------------------------------------------

   public LocalQueue(Serializable id)
   {
      super(id);
   }

   // Channel implementation ----------------------------------------

   public boolean handle(Routable r)
   {
      if (log.isTraceEnabled()) { log.trace(this + " forwarding to router"); }

      Set acks = router.handle(r);

      // the router either returns an empty set or a set containing one element

      if (acks.size() == 1 && ((Acknowledgment)acks.iterator().next()).isPositive())
      {
         if (log.isTraceEnabled()) { log.trace(this + " successful synchronous delivery"); }
         return true;
      }

      return updateAcknowledgments(r, acks);
   }

   // AbstractDestination implementation ----------------------------

   protected AbstractRouter createRouter()
   {
       return new PointToPointRouter("P2PRouter");
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      StringBuffer sb = new StringBuffer("LocalQueue[");
      sb.append(getReceiverID());
      sb.append("]");
      return sb.toString();
   }


}
