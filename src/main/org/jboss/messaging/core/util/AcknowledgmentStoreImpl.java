/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.messaging.core.AcknowledgmentStore;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

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

   protected Serializable storeID;
   protected Serializable state;

   protected Map map;

   // Constructors --------------------------------------------------

   public AcknowledgmentStoreImpl(Serializable id)
   {
      this(id, VALID);
   }

   public AcknowledgmentStoreImpl(Serializable storeID, String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown state: "+state);
      }
      this.storeID = storeID;
      this.state = state;

      map = new HashMap();
   }


   // AcknowledgmentStore implementation ----------------------------

   public Serializable getStoreID()
   {
      return storeID;
   }

   public synchronized void storeNACK(Serializable messageID, Serializable receiverID)
         throws Throwable
   {
      if (state == BROKEN)
      {
         throw new Throwable("THIS IS A THROWABLE THAT SIMULATES "+
                             "THE BEHAVIOUR OF A BROKEN ACKNOWLEDGMENT STORE");
      }

      Map messages = (Map)map.get(receiverID);
      if (messages == null)
      {
         messages = new HashMap();
         map.put(receiverID, messages);
      }
      messages.put(messageID, messageID);
   }

   public synchronized boolean forgetNACK(Serializable messageID, Serializable receiverID)
         throws Throwable
   {
      if (state == BROKEN)
      {
         throw new Throwable("THIS IS A THROWABLE THAT SIMULATES "+
                             "THE BEHAVIOUR OF A BROKEN ACKNOWLEDGMENT STORE");
      }

      Map messages = (Map)map.get(receiverID);
      if (messages == null)
      {
         return false;
      }
      return messages.remove(messageID) != null;
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
