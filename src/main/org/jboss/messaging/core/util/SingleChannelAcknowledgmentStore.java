/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.State;

import java.io.Serializable;
import java.util.Set;

/**
 * An AcknowledgmentStore that stores acknowledgments for a single channel. channelID passed as
 * argument will be ignored.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class SingleChannelAcknowledgmentStore extends InMemoryAcknowledgmentStore
{
   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(SingleChannelAcknowledgmentStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SingleChannelAcknowledgmentStore(Serializable storeID)
   {
      super(storeID);
   }

   // AcknowledgmentStore implementation ----------------------------

   public synchronized void update(Serializable channelID, Serializable messageID, State newState)
         throws Throwable
   {
      // channelID is ignored
      update(map, messageID, newState);
   }

   public synchronized void acknowledge(Serializable channelID,
                                        Serializable messageID,
                                        Serializable receiverID,
                                        String txID)
         throws Throwable
   {
      // channelID is ignored
      acknowledge(map, messageID, receiverID, txID);
   }

   public synchronized void remove(Serializable channelID,
                                   Serializable messageID)
         throws Throwable
   {
      // channelID is ignored
      remove(map, messageID);
   }

   public Set getUnacknowledged(Serializable channelID)
   {
      // channelID is ignored
      return getUnacknowledged(map);
   }

   public boolean hasNACK(Serializable channelID, Serializable messageID)
   {
      // channelID is ignored
      return hasNACK(map, messageID);
   }

   public Set getNACK(Serializable channelID, Serializable messageID)
   {
      // channelID is ignored
      return getNACK(map, messageID);
   }

   public Set getACK(Serializable channelID, Serializable messageID)
   {
      // channelID is ignored
      return getACK(map, messageID);
   }

   public void commit(Serializable channelID, String txID)
   {
      // channelID is ignored
      commit(map, txID);
   }

   public void rollback(Serializable channelID, String txID)
   {
      // channelID is ignored
      rollback(map, txID);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
