/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.logging.Logger;

import java.util.Iterator;

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

   public PointToPointRouter()
   {
      super();
   }

   // AbstractRouter implementation ---------------------------------

   /**
    * @return true if the router successfully delivered the message to one and only one receiver
    *         (handle() invocation on that receiver returned true), false otherwise.
    */
   public boolean handle(Message m)
   {
      synchronized(receivers)
      {
         if (receivers.isEmpty())
         {
            return false;
         }

         if (!passByReference)
         {
            m = (Message)m.clone();
         }

         // iterate over targets and try to send the message until either send succeeds or there are
         //  no targets left
         for(Iterator i = iterator(); i.hasNext(); )
         {
            Receiver r = (Receiver)i.next();
            try
            {
               boolean successful = r.handle(m);
               receivers.put(r, new Boolean(successful));
               if (successful)
               {
                  return true;
               }
            }
            catch(Exception e)
            {
               // broken receiver - log the exception and ignore the receiver
               log.error("The receiver " + r + " cannot handle the message.", e);
            }
         }
      }
      // cannot deliver, let the sender know
      return false;
   }
}
