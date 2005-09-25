/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core.local;

import java.util.Iterator;
import java.util.Set;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.TransactionalChannelSupport;
import org.jboss.messaging.core.PersistenceManager;
import org.jboss.messaging.core.MessageStore;

import javax.transaction.TransactionManager;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class Topic implements Receiver, Distributor
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected Router router;
   
   // Constructors --------------------------------------------------

   public Topic(String name)
   {      
      router = new PointToMultipointRouter();
   }

   // Public --------------------------------------------------------
   
   public Delivery handle(DeliveryObserver sender, Routable r)
   {
      router.handle(sender, r);
      return new SimpleDelivery(true);
   }


   public boolean add(Receiver receiver)
   {
      return router.add(receiver);
   }

   public void clear()
   {
      router.clear();
   }

   public boolean contains(Receiver receiver)
   {
      return router.contains(receiver);
   }

   public Iterator iterator()
   {
      return router.iterator();
   }

   public boolean remove(Receiver receiver)
   {
      return router.remove(receiver);
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
