/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.message.MessageReferenceSupport;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

/**
 * TODO - incomplete implementation - just simulates a MessageStore for testing purposes
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageStoreImpl implements MessageStore
{
   // Constants -----------------------------------------------------

   public static final String VALID = "VALID";
   public static final String BROKEN = "BROKEN";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   protected Serializable storeID;
   protected String state;

   protected Map map;

   // Constructors --------------------------------------------------

   public MessageStoreImpl(Serializable storeID)
   {
      this(storeID, VALID);
   }

   public MessageStoreImpl(Serializable storeID, String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown state: "+state);
      }
      this.storeID = storeID;
      this.state = state;
      map = new HashMap();
   }


   // MessageStore implementation -----------------------------------

   public Serializable getStoreID()
   {
      return storeID;
   }

   public synchronized MessageReference store(Message m) throws Throwable
   {
      if (state == BROKEN)
      {
         throw new Throwable("THIS IS A THROWABLE THAT SIMULATES "+
                             "THE BEHAVIOUR OF A BROKEN MESSAGE STORE");
      }

      Serializable messageID = m.getMessageID();

      if (map.get(messageID) == null)
      {
         map.put(messageID, m);
      }
      return new MessageReferenceSupport(m, storeID);
   }

   public Message retrieve(MessageReference r)
   {
      Serializable storeID = r.getStoreID();
      if (!this.storeID.equals(storeID))
      {
         return null;
      }
      return (Message)map.get(r.getMessageID());
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
