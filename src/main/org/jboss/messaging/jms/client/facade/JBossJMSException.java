/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.jms.client.facade;

import javax.jms.JMSException;

/**
 * A JMS exception that allows for an embedded exception.
 * 
 * @author <a href="mailto:adrian@jboss.org">Adrian Brock</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 *
 * @version $Revision$
 */
public class JBossJMSException
   extends JMSException
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   /**
    * The causing exception
    */
   private Throwable cause;

   // Static --------------------------------------------------------

   /**
    * Handle an exception.
    * 
    * @param t the exception.
    * @return the resultant JMSException.
    */
   public static JMSException handle(Throwable t)
   {
      if (t instanceof JMSException)
      {
         return (JMSException)t;
      }
      return new JBossJMSException("Error", t);
   }

   // Constructors --------------------------------------------------

   /**
    * Construct a new JBossJMSException with the given message.
    *
    * @param message the messagen
    */
   public JBossJMSException(String message)
   {
      super(message);
   }

   /**
    * Construct a new JBossJMSException with the give message and causen
    *
    * @param message the message.
    * @param cause the cause.
    */
   public JBossJMSException(String message, Throwable cause)
   {
      super(message);
      this.cause = cause;
   }

   // Public --------------------------------------------------------

   // Throwable overrides -------------------------------------------

   public Throwable getCause()
   {
      return cause;
   }

   // Protected ------------------------------------------------------

   // Package Private ------------------------------------------------

   // Private --------------------------------------------------------

   // Inner Classes --------------------------------------------------
}
