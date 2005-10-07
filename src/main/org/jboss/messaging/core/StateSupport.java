/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;


import org.jboss.logging.Logger;

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

   private static final Logger log = Logger.getLogger(StateSupport.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected List messageRefs;
   protected List deliveries;


   protected Channel channel;

   // Constructors --------------------------------------------------

   public StateSupport(Channel channel)
   {
      this.channel = channel;
      messageRefs = new ArrayList();
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

   public void add(Delivery d) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("adding " + d); }

      deliveries.add(d);
   }

   public boolean remove(Delivery d) throws Throwable
   {
      if (log.isTraceEnabled()) { log.trace("removing " + d); }

      boolean removed = deliveries.remove(d);
      
      return removed;
   }

   public void add(MessageReference ref) throws Throwable
   {
      messageRefs.add(ref);
   }

   public boolean remove(MessageReference ref)
   {
      boolean removed = messageRefs.remove(ref);
      return removed;
   }

   public List undelivered(Filter filter)
   {
      List undelivered = new ArrayList();
      for(Iterator i = messageRefs.iterator(); i.hasNext(); )
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
      
      List undel = undelivered(filter);
      if (log.isTraceEnabled()) { log.trace("I have " + undel.size() + " undelivered"); }
      result.addAll(undel);
      return result;
   }

   public void clear()
   {
      messageRefs.clear();
      messageRefs = null;
      channel = null;
   }

   // Public --------------------------------------------------------
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------  
   
}
