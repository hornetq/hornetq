/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.core;

import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

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

   private static final Logger log = Logger.getLogger(ReceiverImpl.class);

   public static final String HANDLING = "HANDLING";
   public static final String DENYING = "DENYING";
   public static final String BROKEN = "BROKEN";

   private static final String INVOCATION_COUNT = "INVOCATION_COUNT";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Serializable id;
   private List messages;
   private String state;
   private String futureState;
   private int invocationTowardsFutureState;

   private Map waitingArea;

   // Constructors --------------------------------------------------

   public ReceiverImpl()
   {
      this(HANDLING);
   }

   public ReceiverImpl(String state)
   {
      this("GenericReceiver", state);
   }

   public ReceiverImpl(Serializable id, String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown Receiver state: "+state);
      }
      this.id = id;
      this.state = state;
      messages = new ArrayList();
      waitingArea = new HashMap();
      waitingArea.put(INVOCATION_COUNT, new Integer(0));
   }

   // Recevier implementation ---------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public boolean handle(Routable m)
   {
      try
      {
         if (BROKEN.equals(state))
         {
            // rogue receiver, throws unchecked exceptions
            throw new RuntimeException("THIS IS AN EXCEPTION THAT SIMULATES "+
                                       "THE BEHAVIOUR OF A BROKEN RECEIVER");
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
      finally
      {
         synchronized(waitingArea)
         {
            if (futureState != null && --invocationTowardsFutureState == 0)
            {
               state = futureState;
               futureState = null;
            }

            Integer crt = (Integer)waitingArea.get(INVOCATION_COUNT);
            waitingArea.put(INVOCATION_COUNT, new Integer(crt.intValue() + 1));
            waitingArea.notifyAll();
         }
      }
   }

   // Public --------------------------------------------------------

   public void clear()
   {
      messages.clear();
   }

   /**
    * Blocks until handle() is called for the specified number of times.
    * @param count
    */
   public void waitForHandleInvocations(int count)
   {
      synchronized(waitingArea)
      {
         while(true)
         {
            Integer invocations = (Integer)waitingArea.get(INVOCATION_COUNT);
            if (invocations.intValue() == count)
            {
               return;
            }
            try
            {
               waitingArea.wait(1000);
            }
            catch(InterruptedException e)
            {
               // OK
               log.debug(e);
            }
         }
      }
   }

   public void resetInvocationCount()
   {
      synchronized(waitingArea)
      {
         waitingArea.put(INVOCATION_COUNT, new Integer(0));
         waitingArea.notifyAll();
      }
   }

   public Iterator iterator()
   {
      return messages.iterator();
   }

   public List getMessages()
   {
      return messages;
   }

   public boolean contains(Serializable messageID)
   {
      for(Iterator i = messages.iterator(); i.hasNext(); )
      {
         if (((Routable)i.next()).getMessageID().equals(messageID))
         {
            return true;
         }
      }
      return false;
   }

   public void setState(String state)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown Receiver state: "+state);
      }
      this.state = state;
   }

   /**
    * Sets the given state on the receiver, but only after "invocationCount" handle() invocations.
    * The state changes <i>after</i> the last invocation.
    */
   public void setState(String state, int invocationCount)
   {
      if (!isValid(state))
      {
         throw new IllegalArgumentException("Unknown Receiver state: "+state);
      }
      futureState = state;
      invocationTowardsFutureState = invocationCount;
   }


   public String getState()
   {
      return state;
   }


   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }
      if (!(o instanceof ReceiverImpl))
      {
         return false;
      }
      ReceiverImpl that = (ReceiverImpl)o;

      if (id == null)
      {
         return that.id == null;
      }
      return id.equals(that.id);
   }

   public int hashCode()
   {
      if (id == null)
      {
         return 0;
      }
      return id.hashCode();
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
