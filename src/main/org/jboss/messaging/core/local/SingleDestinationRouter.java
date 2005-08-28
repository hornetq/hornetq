/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.local;

import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Delivery;
import org.jboss.logging.Logger;

import java.util.Set;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class SingleDestinationRouter implements Router
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SingleDestinationRouter.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Receiver receiver;

   // Constructors --------------------------------------------------

   // Router implementation -----------------------------------------

   public synchronized Set handle(DeliveryObserver observer, Routable routable)
   {
      if (receiver == null)
      {
         return Collections.EMPTY_SET;
      }

      Set deliveries = new HashSet(1);
      try
      {
         Delivery d = receiver.handle(observer, routable);

         if (d != null)
         {
            deliveries.add(d);
         }
      }
      catch(Throwable t)
      {
         // broken receiver - log the exception and ignore it
         log.error("The receiver " + receiver + " is broken" + t);
      }
      return deliveries;
   }

   public synchronized boolean add(Receiver r)
   {
      if (receiver == r)
      {
         return false;
      }
      receiver = r;
      return true;
   }


   public synchronized boolean remove(Receiver r)
   {
      boolean result = false;
      if (receiver == r)
      {
         result = true;
      }
      receiver = null;
      return result;
   }

   public synchronized void clear()
   {
      receiver = null;
   }

   public synchronized boolean contains(Receiver r)
   {
      return receiver == r;
   }

   public synchronized Iterator iterator()
   {
      if (receiver == null)
      {
         return Collections.EMPTY_LIST.iterator();
      }
      List l = new ArrayList();
      l.add(receiver);
      return l.iterator(); 
   }




   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
