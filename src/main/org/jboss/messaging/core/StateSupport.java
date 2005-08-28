/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;


import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Unreliable (in-memory), non transactional channel state implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
abstract class StateSupport implements State
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected List messages;
   protected List deliveries;


   protected Channel channel;

   // Constructors --------------------------------------------------

   public StateSupport(Channel channel)
   {
      this.channel = channel;
      messages = new ArrayList();
      deliveries = new ArrayList();
   }

   // State implementation -----------------------------------

   public boolean isReliable()
   {
      return false;
   }

   public boolean isTransactional()
   {
      return false;
   }

   // There are no non-transactional delivery additions, so no add(Delivery d) here

   public boolean remove(Delivery d) throws Throwable
   {
      return deliveries.remove(d);
   }

   public void add(Routable r) throws Throwable
   {
      if (r.isReliable())
      {
         throw new IllegalStateException("Cannot reliably hold a reliable message");
      }
      messages.add(r);
   }

   public boolean remove(Routable r)
   {
      return messages.remove(r);
   }

   public List undelivered(Filter filter)
   {
      List undelivered = new ArrayList();
      for(Iterator i = messages.iterator(); i.hasNext(); )
      {
         Routable r = (Routable)i.next();
         if (filter == null || filter.accept(r))
         {
            undelivered.add(r);
         }
      }
      return undelivered;
   }

   public List browse(Filter filter)
   {
      List result = delivering(filter);
      result.addAll(undelivered(filter));
      return result;
   }

   public void clear()
   {
      messages.clear();
      messages = null;
      channel = null;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
