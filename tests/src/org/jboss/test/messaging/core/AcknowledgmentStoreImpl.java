/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.AcknowledgmentStore;
import org.jboss.messaging.core.AcknowledgmentStore;

import java.io.Serializable;

/**
 * TODO - incomplete implementation - just simulates an AcknowledgmentStore for testing purposes
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class AcknowledgmentStoreImpl implements AcknowledgmentStore
{
   // Constants -----------------------------------------------------

   public static final String VALID = "VALID";
   public static final String BROKEN = "BROKEN";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable id;
   protected Serializable state;

   // Constructors --------------------------------------------------

   public AcknowledgmentStoreImpl(Serializable id)
   {
      this(id, VALID);
   }

   public AcknowledgmentStoreImpl(Serializable id, String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown state: "+state);
      }
      this.id = id;
      this.state = state;
   }


   // AcknowledgmentStore implementation ----------------------------

   public Serializable getStoreID()
   {
      return id;
   }

   public void storeNACK(Serializable messageID, Serializable receiverID) throws Throwable
   {
      if (state == BROKEN)
      {
         throw new Throwable("THIS IS A THROWABLE THAT SIMULATES "+
                             "THE BEHAVIOUR OF A BROKEN ACKNOWLEDGMENT STORE");
      }
   }

   public boolean forgetNACK(Serializable messageID, Serializable receiverID) throws Throwable
   {
      if (state == BROKEN)
      {
         throw new Throwable("THIS IS A THROWABLE THAT SIMULATES "+
                             "THE BEHAVIOUR OF A BROKEN ACKNOWLEDGMENT STORE");
      }
      return true;
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
