/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.delegate;

import javax.jms.Message;
import javax.jms.JMSException;

/**
 * Provider of method definitions for all generic invocations.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class GenericTarget
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   public Message receive() throws JMSException
   {
      throw new JMSException("The call wasn't supposed to get here");
   }

   public Message receive(long timeout) throws JMSException
   {
      throw new JMSException("The call wasn't supposed to get here");
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
