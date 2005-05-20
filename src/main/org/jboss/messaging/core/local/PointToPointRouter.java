/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.util.AcknowledgmentImpl;

import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.io.Serializable;

/**
 * A Router that synchronously delivers the message to one and only one receiver.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PointToPointRouter extends AbstractRouter
{
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public PointToPointRouter(Serializable id)
   {
      super(id);
      log = Logger.getLogger(PointToPointRouter.class);
   }

   // AbstractRouter implementation ---------------------------------


   public Set handle(Routable r, Set receiverIDs)
   {
      Set acks = new HashSet();

      synchronized(this)
      {
         for(Iterator i = iterator(receiverIDs); i.hasNext(); )
         {
            Serializable receiverID = (Serializable)i.next();
            Receiver receiver = (Receiver)receivers.get(receiverID);
            try
            {
               if (log.isTraceEnabled()) { log.trace(this + " attempting to deliver to " + receiverID); }

               boolean successful = receiver.handle(r);

               if (log.isTraceEnabled()) { log.trace(this + ": receiver " + receiverID + (successful ? " ACKed" : " NACKed") + " the message"); }

               acks.add(new AcknowledgmentImpl(receiverID, successful));
               break;
            }
            catch(Throwable t)
            {
               // broken receiver - log the exception and ignore the receiver
               log.error("The receiver " + receiverID + " is broken", t);
            }
         }
      }
      return acks;
   }

   // Public -----------------------------------------------------------


   public String toString()
   {
      StringBuffer sb = new StringBuffer("P2PRouter[");
      sb.append(getRouterID());
      sb.append("]");
      return sb.toString();
   }

   // Private ----------------------------------------------------------

}
