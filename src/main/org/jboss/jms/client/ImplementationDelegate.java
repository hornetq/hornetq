/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.naming.Reference;

/**
 * The implementation
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
 * @version $Revision$
 */
public interface ImplementationDelegate 
{
   // Constants -----------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Create a connection delegate
    * 
    * @param the user name
    * @param the password
    * @return the connection delegate
    * @throws JMSException for any error
    */
   ConnectionDelegate createConnection(String userName, String password) throws JMSException;

   /**
    * Get a reference to the connection factory
    * 
    * @return a Reference
    * @throws NamingException for any error
    */
   Reference getReference() throws NamingException;

   // Inner Classes --------------------------------------------------
}
