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
 * A Router that synchronously delivers the Routable to all its receivers. What will be actually
 * passed is the reference to the incoming Routable.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PointToMultipointRouter extends AbstractRouter
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PointToMultipointRouter.class); 

   // Constructors --------------------------------------------------

   public PointToMultipointRouter(Serializable id)
   {
      super(id);
   }

   // AbstractRouter implementation ---------------------------------

   /**
    * The method tries to deliver the message to all receivers connected to the router. If all of
    * them acknowledge the message, the method returns true. If there is at least one who doesn't,
    * the method returns false. In this case, it is the sender's responsibility to find out which
    * receiver did not acknowledge the message and retry delivery.
    * <p>
    * The sender can obtain the acknowledgment situation on a per-receiver basis using the
    * acknowledged() method.
    *
    * @see org.jboss.messaging.core.Distributor#acknowledged(Serializable)
    */
   public boolean handle(Routable r)
   {

      boolean allSuccessful = true;
      synchronized(this)
      {
         if (receivers.isEmpty())
         {
            return false;
         }
         for (Iterator i = iterator(); i.hasNext(); )
         {
            Serializable receiverID = (Serializable)i.next();
            Receiver receiver = (Receiver)receivers.get(receiverID);
            try
            {
               boolean successful = receiver.handle(r);
               allSuccessful &= successful;
               acknowledgments.put(receiverID, new Boolean(successful));
            }
            catch(Exception e)
            {
               // broken receiver - log the exception and ignore the receiver
               allSuccessful = false;
               log.error("The receiver " + receiverID + " cannot handle the message.", e);
            }
         }
      }
      return allSuccessful;
   }

   // Public --------------------------------------------------------

}
