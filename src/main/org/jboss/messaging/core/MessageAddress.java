/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

/**
 * A message address.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @version $Revision$
 */
public interface MessageAddress
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Get the name
    * 
    * @return the name
    */
   String getName();
   
   // Inner Classes --------------------------------------------------
}
