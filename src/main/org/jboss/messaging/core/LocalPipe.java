/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Channel;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.interfaces.Routable;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Collections;
import java.util.HashSet;

/**
 * A local Channel with only one output. Both the input and the output endpoints are in the same
 * address space.
 *
 * <p>
 * Only one receiver can be connected to this channel at a time. Synchronous delivery is attempted,
 * but if it is not possible, the LocalPipe will hold the message (subject to the asynchronous
 * behavior conditions).
 *
 * @see org.jboss.messaging.interfaces.Channel
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class LocalPipe extends ChannelSupport
{
   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(LocalPipe.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Receiver receiver = null;

   /** List of unacked Routables */
   protected List unacked = null;

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
      this(id, Channel.SYNCHRONOUS, receiver);
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
      unacked = new ArrayList();
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
      catch(Exception e)
      {
         log.warn("The receiver " + receiverID + " failed to handle the message", e);
      }
      return storeNACKedMessage(r, receiverID);
   }

   public boolean deliver()
   {
      synchronized(channelLock)
      {
         // try to flush the message store
         for(Iterator i = unacked.iterator(); i.hasNext(); )
         {
            Routable r = (Routable)i.next();
            try
            {
               if (receiver.handle(r))
               {
                  i.remove();
               }
            }
            catch(Throwable t)
            {
               // most likely the receiver is broken, don't insist
               break;
            }
         }
         return unacked.isEmpty();
      }
   }

   public boolean hasMessages()
   {
      synchronized(channelLock)
      {
         return !unacked.isEmpty();
      }
   }

   public Set getUnacknowledged()
   {
      synchronized(channelLock)
      {
         if (unacked.isEmpty())
         {
            return Collections.EMPTY_SET;
         }
         Set s = new HashSet();
         for(Iterator i = unacked.iterator(); i.hasNext(); )
         {
            Serializable mid = ((Routable)i.next()).getMessageID();
            if (!s.add(mid))
            {
               log.warn("Duplicate message ID in the unacknowledged list: " + mid);
            }
         }
         return s;
      }
   }


   // ChannelSupport implementation ---------------------------------

   protected void storeNACKedMessageLocally(Routable r, Serializable recID)
   {
      if (log.isTraceEnabled()) {log.trace("store NACK locally: "+r.getMessageID()+" from "+recID);}
      // I don't care about recID, since it's my only receiver
      unacked.add(r);
   }


   // Public --------------------------------------------------------

   /**
    * @return the receiver connected to the pipe or null if there is no Receiver.
    */
   public Receiver getReceiver()
   {
      synchronized(channelLock)
      {
         return receiver;
      }
   }

   /**
    * Connect a receiver to the pipe.
    */
   public void setReceiver(Receiver r)
   {
      synchronized(channelLock)
      {
         receiver = r;

         // adding a Receiver triggers an asynchronous delivery attempt
         // TODO Is this good?
         if (hasMessages())
         {
            deliver();
         }
      }
   }
}
