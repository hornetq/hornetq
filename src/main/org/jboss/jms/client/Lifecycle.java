/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import javax.jms.JMSException;

/**
 * The lifecycle
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface Lifecycle 
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Notify about to close
    * 
    * @throws JMSException for any error
    */
   void closing() throws JMSException;

   /**
    * Close the delegate
    * 
    * @throws JMSException for any error
    */
   void close() throws JMSException;

   // Inner Classes --------------------------------------------------
}
