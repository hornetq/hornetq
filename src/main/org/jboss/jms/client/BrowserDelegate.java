/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import java.util.List;

import javax.jms.JMSException;

/**
 * The implementation of a browser
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface BrowserDelegate
   extends Lifecycle 
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Browse the messages
    * 
    * @return a list of messages
    * @throws JMSException for any error
    */
   List browse() throws JMSException;

   // Inner Classes --------------------------------------------------
}
