/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms;

import javax.jms.JMSException;

/**
 * A JMS exception that allows for an embedded exception 
 * 
 * @author <a href="mailto:adrian@jboss.org>Adrian Brock</a>
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
    * Handle an exception
    * 
    * @param t the exception
    * @return the resultant JMSException
    */
   public static JMSException handle(Throwable t)
   {
      if (t instanceof JMSException)
         return (JMSException) t;
      return new JBossJMSException("Error",t);
   }

   // Constructors --------------------------------------------------

   /**
    * Construct a new JBossJMSException with the give message
    *
    * @param message the message
    */
   public JBossJMSException(String message)
   {
      super(message);
   }

   /**
    * Construct a new JBossJMSException with the give message and
    * cause
    *
    * @param message the message
    * @param cause the cause
    */
   public JBossJMSException(String message, Throwable cause)
   {
      super(message);
      this.cause = cause;
   }

   // Public --------------------------------------------------------

   // X implementation ----------------------------------------------

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
