/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.util.Enumeration;

import javax.jms.JMSException;

/**
 * The implementation of a browser
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface BrowserDelegate 
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

   /**
    * Browse the messages
    * 
    * @return an Enumeration of messages
    * @throws JMSException for any error
    */
   Enumeration getEnumeration() throws JMSException;

   // Inner Classes --------------------------------------------------
}
