/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.MessageStore;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.message.MessageReferenceSupport;
import org.jboss.messaging.core.Message;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.messaging.util.NotYetImplementedException;

import java.io.Serializable;

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
   protected Serializable id;
   protected String state;

   // Constructors --------------------------------------------------

   public MessageStoreImpl(Serializable id)
   {
      this(id, VALID);
   }

   public MessageStoreImpl(Serializable id, String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown state: "+state);
      }
      this.id = id;
      this.state = state;
   }


   // MessageStore implementation -----------------------------------

   public Serializable getStoreID()
   {
      return id;
   }

   public MessageReference store(Message m) throws Throwable
   {
      if (state == BROKEN)
      {
         throw new Throwable("THIS IS A THROWABLE THAT SIMULATES "+
                             "THE BEHAVIOUR OF A BROKEN MESSAGE STORE");
      }
      return new MessageReferenceSupport(m, id);
   }

   public Message retrieve(MessageReference r)
   {
      throw new NotYetImplementedException();
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
