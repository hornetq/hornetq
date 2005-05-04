/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.local;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Routable;

import java.io.Serializable;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

/**
 * An exended set of functionalities for single output channels (LocalPipes, Pipes). It assumes
 * that unacknowledged messages are kept in a List.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class SingleOutputChannelSupport extends ChannelSupport
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SingleOutputChannelSupport.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   /** List of unacked Routables */
   protected List unacked = null;

   // Constructors --------------------------------------------------

   public SingleOutputChannelSupport()
   {
      // the default behaviour is SYNCHRONOUS
      super();
      unacked = new ArrayList();
   }

   public SingleOutputChannelSupport(boolean mode)
   {
      super(mode);
      unacked = new ArrayList();
   }

   // Channel implementation ----------------------------------------

   public boolean hasMessages()
   {
      lock();

      try
      {
         return !unacked.isEmpty();
      }
      finally
      {
         unlock();
      }
   }

   public Set getUnacknowledged()
   {
      lock();
      
      try
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
      finally
      {
         unlock();
      }
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected void storeNACKedMessageLocally(Routable r, Serializable recID)
   {
      if (log.isTraceEnabled()) {log.trace(this + ": store unreliably NACKed message ("+ r.getMessageID() + ") for " + recID);}

      // the channel's lock is already acquired when invoking this method

      // I don't care about recID, since it's my only receiver
      unacked.add(r);
   }

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
