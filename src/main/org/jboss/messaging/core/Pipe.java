/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Channel;
import org.jboss.messaging.interfaces.Receiver;
import org.jboss.messaging.interfaces.MessageSet;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.tools.MessageSetImpl;

/**
 * A Channel with only one output.
 *
 * <p>
 * Only one receiver can be connected to this channel at a time. Synchronous delivery is attempted,
 * but if it is not possible, the Pipe will hold the message.
 *
 * <p>
 * The asynchronous behaviour can be turned off by seting "synchronous" flag true. If the pipe is
 * "synchronous" and the synchronous delivery fails, the overall delivery fails and the pipe won't
 * hold the message.
 *
 * @see org.jboss.messaging.interfaces.Channel
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Pipe implements Channel
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected boolean synchronous = false;
   protected Receiver receiver = null;
   protected MessageSet messages = null;

   // Constructors --------------------------------------------------

   /**
    * The default message handling mode is asynchronous.
    */
   public Pipe()
   {
      this(false, null);
   }

   /**
    * The default message handling mode is asynchronous.
    *
    * @param receiver - the pipe's output receiver.
    */
   public Pipe(Receiver receiver)
   {
      this(false, receiver);
   }

   /**
    * @param mode - message handling mode. Use true for synchronous handling,
    *        false for asynchronous.
    */
   public Pipe(boolean mode)
   {
      this(mode, null);
   }

   /**
    *
    * @param mode - message handling mode. Use true for synchronous handling,
    *        false for asynchronous.
    * @param receiver - the pipe's output receiver.
    */
   public Pipe(boolean mode, Receiver receiver)
   {
      synchronous = mode;
      this.receiver = receiver;
      messages = new MessageSetImpl();
   }

   // Channel implementation ----------------------------------------

   public boolean handle(Message m)
   {

      if (receiver != null && receiver.handle(m))
      {
         // successful synchronous delivery
         return true;
      }

      if (synchronous)
      {
         // synchronous delivery failed
         return false;
      }
      else
      {
         // if asynhronous, keep the message until instructed to re-deliver
         // TODO lock
         messages.add(m);
         return true;
      }
   }

   public boolean deliver()
   {
      messages.lock();

      try
      {
         // try to flush the message store
         while(true)
         {
            Message m = messages.get();
            if (m == null)
            {
               return true;
            }
            synchronized(this)
            {
               if (receiver == null || !receiver.handle(m))
               {
                  return false;
               }
               messages.remove(m);
            }
         }
      }
      finally
      {
         messages.unlock();
      }
   }

   public boolean hasMessages()
   {
      // TODO lock
      return messages.get() != null;
   }

   public boolean setSynchronous(boolean synchronous)
   {
      // TODO lock
      if (synchronous && hasMessages())
      {
         return false;
      }
      this.synchronous = synchronous;
      return true;
   }

   public boolean isSynchronous()
   {
       return synchronous;
   }

   // Public --------------------------------------------------------

   /**
    * @return the receiver connected to the pipe or null if there is no Receiver.
    */
   public synchronized Receiver getReceiver()
   {
      return receiver;
   }

   /**
    * Connect a receiver to the pipe.
    */
   public synchronized void setReceiver(Receiver r)
   {
       receiver = r;

      // adding a Receiver triggers an asynchronous delivery attempt
      if (hasMessages())
      {
         deliver();
      }
   }

   // DEBUG ---------------------------------------------------------

   public String dump()
   {
      StringBuffer sb =
            new StringBuffer("Pipe(").append(synchronous?"SYNCHRONOUS":"ASYNCHRONOUS").
            append("): messages ");

      sb.append(((MessageSetImpl)messages).dump());
      sb.append(" receiver: ");
      sb.append(getReceiver());
      return sb.toString();
   }
}
