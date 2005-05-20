/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.util.InMemoryAcknowledgmentStore;

import java.io.Serializable;
import java.util.Set;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class TestAcknowledgmentStore extends InMemoryAcknowledgmentStore
{
   // Constants -----------------------------------------------------

   public static final String VALID = "VALID";
   public static final String BROKEN = "BROKEN";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable state;

   // Constructors --------------------------------------------------

   public TestAcknowledgmentStore(Serializable id)
   {
      this(id, VALID);
   }

   public TestAcknowledgmentStore(Serializable storeID, String state)
   {
      super(storeID);
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown state: "+state);
      }
      this.state = state;
   }


   // AcknowledgmentStore implementation ----------------------------

   public synchronized void updateAcknowledgments(Serializable channelID,
                                                  Serializable messageID,
                                                  Set receiverIDs)
         throws Throwable
   {
      if (state == BROKEN)
      {
         throw new Throwable("THIS IS A THROWABLE THAT SIMULATES "+
                             "THE BEHAVIOUR OF A BROKEN ACKNOWLEDGMENT STORE");
      }

      super.updateAcknowledgments(messageID, channelID, receiverIDs);
   }

   // Public --------------------------------------------------------

   public void setState(String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown state: "+state);
      }
      this.state = state;
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------


   // Private -------------------------------------------------------

   private static boolean isValid(String state)
   {
      if (VALID.equals(state) ||
          BROKEN.equals(state))
      {
         return true;
      }
      return false;
   }


   // Inner classes -------------------------------------------------
}
