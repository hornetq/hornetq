/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.channel.interfaces;

import org.jboss.messaging.interfaces.MessageReference;

/**
 * A channel.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public interface Channel
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Send a message to the channel.
    * 
    * @param reference the message reference to send
    */
   void send(MessageReference reference);

   /**
    * Receive a message from the channel.
    * 
    * @param wait the length of time to wait for a message if there are none
    *        immediately available, use -1 for no wait.
    * @return a message or null if there are no messages
    */
   MessageReference receive(long wait);
   
   /**
    * Close the channel
    */
   void close();
   
   // Inner Classes --------------------------------------------------
}
