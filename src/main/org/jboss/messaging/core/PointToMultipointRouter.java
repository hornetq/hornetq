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
 * A Router that synchronously delivers the message (or a copy of it) to all its receivers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class PointToMultipointRouter extends AbstractRouter
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Logger log = Logger.getLogger(getClass());

   // Constructors --------------------------------------------------

   /**
    *
    * @param passByReference - if true, the router sends a reference to the same message to all its
    *        receivers, otherwise the router clones the message and sends distinct copies to the
    *        receivers.
    */
   public PointToMultipointRouter(boolean passByReference)
   {
      super();
      this.passByReference = passByReference;
   }

   /**
    * By default, the router passes the message by reference.
    */
   public PointToMultipointRouter()
   {
      this(true);
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
    * @see org.jboss.messaging.interfaces.Distributor#acknowledged(Receiver)
    */
   public boolean handle(Message m)
   {

      boolean allSuccessful = true;
      synchronized(receivers)
      {
         if (receivers.isEmpty())
         {
            return false;
         }
         Message n;
         for (Iterator i = iterator(); i.hasNext(); )
         {
            n = m;
            Receiver r = (Receiver)i.next();
            if (!passByReference)
            {
               n = (Message)m.clone();
            }
            try
            {
               boolean successful = r.handle(n);
               allSuccessful &= successful;
               receivers.put(r, new Boolean(successful));
            }
            catch(Exception e)
            {
               // broken receiver - log the exception and ignore the receiver
               allSuccessful = false;
               log.error("The receiver " + r + " cannot handle the message.", e);
            }
         }
      }
      return allSuccessful;
   }

   // Public --------------------------------------------------------

}
