/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.interfaces;

/**
 * A Channel is a message delivery mechanism that synchronously/asynchronously forwards a Message
 * to the Channel's Receivers.
 *
 * <p>
 * The Channel's default behavior is synchronous. The Channel attempts synchronous delivery and if
 * this is not possible, it holds the message and attempts re-delivery when the deliver() method is
 * called. A Receiver never explicitely pulls a message, it only declares its availability, should
 * a message arrive. It's the cannel that pushes the message to the Recevier.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Channel extends Receiver
{
   /**
    * Attempt asynchronous delivery of messages being held by the Channel.
    *
    * @return false if there are still messages to be delivered after the call completes, true
    *         otherwise.
    */
   public boolean deliver();

   /**
    * @return true if the Channel holds at least one undelivered message, false otherwise.
    */
   public boolean hasMessages();

   /**
    * Configure the message handling mode for this channel.
    *
    * <p>
    * Switching the channel from asynchronous to synchronous mode will fail if the channel contains
    * undelivered messages and the method invocation will return false. 
    *
    * @param b - true for synchronous delivery, false for asynchronous delivery.
    *
    * @return true if the configuration completed successfully, false otherwise.
    */
   public boolean setSynchronous(boolean b);

   /**
    * @return true is the Channel was configured for synchronous handling.
    */
   public boolean isSynchronous();

}


