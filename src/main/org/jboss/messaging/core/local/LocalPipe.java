/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Channel;
import org.jboss.messaging.core.Routable;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A local Channel with only one output. Both the input and the output endpoints are in the same
 * address space.
 *
 * <p>
 * Only one receiver can be connected to this channel at a time. Synchronous delivery is attempted,
 * but if it is not possible, the LocalPipe will hold the message (subject to the asynchronous
 * behavior conditions).
 *
 * @see org.jboss.messaging.core.Channel
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalPipe extends SingleOutputChannelSupport
{
   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(LocalPipe.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Receiver receiver = null;

   protected Serializable id;

   // Constructors --------------------------------------------------

   /**
    * The default message handling mode is synchronous.
    */
   public LocalPipe(Serializable id)
   {
      this(id, Channel.SYNCHRONOUS, null);
   }

   /**
    * The default message handling mode is synchronous.
    *
    * @param receiver - the pipe's output receiver.
    */
   public LocalPipe(Serializable id, Receiver receiver)
   {
      this(id, SYNCHRONOUS, receiver);
   }

   /**
    * @param mode - message handling mode. Use true for synchronous handling, false for
    *        asynchronous.
    */
   public LocalPipe(Serializable id, boolean mode)
   {
      this(id, mode, null);
   }

   /**
    * @param mode - message handling mode. Use true for synchronous handling, false for
    *        asynchronous.
    * @param receiver - the pipe's output receiver.
    */
   public LocalPipe(Serializable id, boolean mode, Receiver receiver)
   {
      super(mode);
      this.id = id;
      this.receiver = receiver;
   }

   // Channel implementation ----------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean handle(Routable r)
   {
      Serializable receiverID = null;
      try
      {
         receiverID = receiver.getReceiverID();
         if (receiver.handle(r))
         {
            // successful synchronous delivery
            return true;
         }
      }
      catch(Throwable e)
      {
         log.warn("The receiver " + receiverID + " failed to handle the message", e);
      }
      return storeNACKedMessage(r, receiverID);
   }

   public boolean deliver()
   {
      lock();

      if (log.isTraceEnabled()) { log.trace("asynchronous delivery triggered on " + getReceiverID()); }

      try
      {
         // try to flush the message store
         for(Iterator i = unacked.iterator(); i.hasNext(); )
         {
            Routable r = (Routable)i.next();
            try
            {

               if (System.currentTimeMillis() > r.getExpirationTime())
               {
                  // message expired
                  log.warn("Message " + r.getMessageID() + " expired by " + (System.currentTimeMillis() - r.getExpirationTime()) + " ms");
                  i.remove();
                  continue;
               }

               if (receiver.handle(r))
               {
                  i.remove();
               }
            }
            catch(Throwable t)
            {
               // most likely the receiver is broken, don't insist
               log.warn("The receiver " + (receiver == null ? "null" : receiver.getReceiverID()) +
                        " failed to handle the message", t);
               break;
            }
         }
         return unacked.isEmpty();
      }
      finally
      {
         unlock();
      }
   }

   // Public --------------------------------------------------------

   /**
    * @return the receiver connected to the pipe or null if there is no Receiver.
    */
   public Receiver getReceiver()
   {
      lock();

      try
      {
         return receiver;
      }
      finally
      {
         unlock();
      }
   }

   /**
    * Connect a receiver to the pipe.
    */
   public void setReceiver(Receiver r)
   {
      lock();

      try
      {
         receiver = r;

         // adding a Receiver triggers an asynchronous delivery attempt
         // TODO Is this good?
         if (hasMessages())
         {
            deliver();
         }
      }
      finally
      {
         unlock();
      }
   }
}
