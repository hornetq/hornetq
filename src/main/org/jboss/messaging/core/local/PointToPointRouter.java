/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.local.AbstractRouter;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Receiver;

import java.util.Iterator;
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

   protected Logger log = Logger.getLogger(getClass());

   // Constructors --------------------------------------------------

   public PointToPointRouter(Serializable id)
   {
      super(id);
   }

   // AbstractRouter implementation ---------------------------------

   /**
    * @return true if the router successfully delivered the message to one and only one receiver
    *         (handle() invocation on that receiver returned true), false otherwise.
    */
   public boolean handle(Routable r)
   {
      synchronized(this)
      {
         if (receivers.isEmpty())
         {
            return false;
         }

         // iterate over targets and try to send the message until either send succeeds or there are
         //  no targets left
         for(Iterator i = iterator(); i.hasNext(); )
         {
            Serializable receiverID = (Serializable)i.next();
            Receiver receiver = (Receiver)receivers.get(receiverID);
            try
            {
               boolean successful = receiver.handle(r);
               acknowledgments.put(receiverID, new Boolean(successful));
               if (successful)
               {
                  return true;
               }
            }
            catch(Exception e)
            {
               // broken receiver - log the exception and ignore the receiver
               log.error("The receiver " + receiverID + " cannot handle the message.", e);
            }
         }
      }
      // cannot deliver, let the sender know
      return false;
   }
}
