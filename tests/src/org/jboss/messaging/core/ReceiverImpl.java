/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.interfaces.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/**
 * A simple Receiver implementation that consumes messages by storing them internally. Used for
 * testing.
 * <p>
 * The receiver can be configured to properly handle messages, to deny messages and to behave as
 * "broken" - to throw a RuntimeException during the handle() call.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReceiverImpl implements Receiver
{
   // Constants -----------------------------------------------------

   public static final String HANDLING = "HANDLING";
   public static final String DENYING = "DENYING";
   public static final String BROKEN = "BROKEN";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private List messages;
   private String state;

   // Constructors --------------------------------------------------

   public ReceiverImpl()
   {
      this(HANDLING);
   }

   public ReceiverImpl(String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown Receiver state: "+state);
      }
      this.state = state;
      messages = new ArrayList();
   }

   // Recevier implementation ---------------------------------------

   public boolean handle(Message m)
   {
      if (BROKEN.equals(state))
      {
         // rogue receiver, throws unchecked exceptions
         throw new RuntimeException();
      }
      if (DENYING.equals(state))
      {
         // politely tells that it cannot handle the message
         return false;
      }
      // Handling receiver
      if (m == null)
      {
         return false;
      }
      messages.add(m);
      return true;
   }

   // Public --------------------------------------------------------

   public void clear()
   {
      messages.clear();
   }

   public Iterator iterator()
   {
      return messages.iterator();
   }

   public void setState(String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown Receiver state: "+state);
      }
      this.state = state;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private static boolean isValid(String state)
   {
      if (HANDLING.equals(state) ||
          DENYING.equals(state) ||
          BROKEN.equals(state))
      {
         return true;
      }
      return false;
   }
}
