/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.interfaces;

/**
 * A consumer of messages.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public interface Consumer
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Check whether the consumer accepts the message.
    * 
    * @param reference the message reference
    * @param active whether to check for activity, pass false
    *        to test for acceptance, true when extra checks
    *        for an active consumer are required.
    * @return true when it accepts the message, false otherwise
    */
   boolean accepts(MessageReference reference, boolean active);

   /**
    * Invoked when a message needs consuming
    * 
    * @param reference the message reference
    */
   void onMessage(MessageReference reference);
   
   // Inner Classes --------------------------------------------------
}
