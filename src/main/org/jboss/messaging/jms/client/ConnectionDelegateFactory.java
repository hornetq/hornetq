/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client;

import javax.jms.JMSException;
import javax.naming.NamingException;
import javax.naming.Reference;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface ConnectionDelegateFactory
{
   /**
    * Create a connection delegate.
    *
    * @param userName the user name.
    * @param password the password.
    * @return the connection delegate.
    * @throws JMSException for any error.
    */
   ConnectionDelegate createConnectionDelegate(String userName, String password)
         throws JMSException;

   /**
    * Get a reference to the connection factory.
    *
    * @return a Reference.
    * @throws NamingException for any error.
    */
   Reference getReference() throws NamingException;

}
