/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Receiver;
import org.jboss.logging.Logger;

import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.io.Serializable;

/**
 * A Channel with a routing policy in place. It delegates the routing policy to a Router instance.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class AbstractDestination
      extends MultipleOutputChannelSupport implements Distributor
{

   private static final Logger log = Logger.getLogger(AbstractDestination.class);

   // Attributes ----------------------------------------------------

   protected AbstractRouter router;
   protected Serializable id;

   // Constructors --------------------------------------------------

   /**
    * By default, a Destination is an asynchronous channel.
    * @param id
    */
   protected AbstractDestination(Serializable id)
   {
      this(id, Channel.ASYNCHRONOUS);
   }

   protected AbstractDestination(Serializable id, boolean mode)
   {
      super(mode);
      router = createRouter();
      this.id = id;
   }

   // Channel implementation ----------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean deliver()
   {
      lock();

      if (log.isTraceEnabled())
      {
         log.trace(id + " asynchronous delivery triggered, " +
                   localAcknowledgmentStore.getUnacknowledged(getReceiverID()).size() +
                   " messages to deliver");
      }

      Set nackedMessages = new HashSet();

      try
      {
         for(Iterator i = localAcknowledgmentStore.getUnacknowledged(getReceiverID()).iterator();
             i.hasNext();)
         {
            nackedMessages.add(i.next());
         }
      }
      finally
      {
         unlock();
      }

      // flush unacknowledged messages

      for(Iterator i = nackedMessages.iterator(); i.hasNext(); )
      {
         Serializable messageID = (Serializable)i.next();
         Routable r = (Routable)messages.get(messageID);

         if (System.currentTimeMillis() > r.getExpirationTime())
         {
            // message expired
            log.warn("Message " + r.getMessageID() + " expired by " + (System.currentTimeMillis() - r.getExpirationTime()) + " ms");
            removeLocalMessage(messageID);
            i.remove();
            continue;
         }

         if (log.isTraceEnabled()) { log.trace(this + " attempting to redeliver " + r); }

         Set targets = localAcknowledgmentStore.getNACK(getReceiverID(), messageID);
         Set acks = router.handle(r, targets);
         updateLocalAcknowledgments(r, acks);
      }

      return !hasMessages();
   }

   // Distributor interface -----------------------------------------

   public boolean add(Receiver r)
   {
      if (!router.add(r))
      {
         return false;
      }

      // adding a Receiver triggers an asynchronous delivery attempt
      if (hasMessages())
      {
         deliver();
      }
      return true;
   }

   public Receiver get(Serializable receiverID)
   {
      return router.get(receiverID);
   }

   public Receiver remove(Serializable receiverID)
   {
      return router.remove(receiverID);
   }

   public boolean contains(Serializable receiverID)
   {
      return router.contains(receiverID);
   }

   public Iterator iterator()
   {
      return router.iterator();
   }

   public void clear()
   {
      router.clear();
   }

   // Public --------------------------------------------------------

   // Protected -----------------------------------------------------

   protected abstract AbstractRouter createRouter();

}




