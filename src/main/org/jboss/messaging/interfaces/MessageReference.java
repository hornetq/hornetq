/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.interfaces;

/**
 * A message reference provides basic information about a message.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public interface MessageReference
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Get the message id
    * 
    * @return the message id
    */
   Comparable getMessageID();
   
   /**
    * Get the message priority
    * 
    * @return the priority
    */
   int getMessagePriority();
   
   /**
    * Get the message address
    * 
    * @return the address
    */
   MessageAddress getMessageAddress();
   
   /**
    * Does the message require guaranteed delivery?
    * 
    * @return true for guaranteed delivery, false otherwise
    */
   boolean isGuaranteed();

   /**
    * Release a reference that is no longer used
    */
   void release();
   
   // Inner Classes --------------------------------------------------
}
