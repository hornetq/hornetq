/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.client.container;

/**
 * Carrier for other exceptions. When catching such an exception, the JMSExceptionInterceptor
 * unwraps the payload exception and throws that one.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class WrapperException extends Exception
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------

   public WrapperException(Throwable payload)
   {
      super(null, payload);
   }

   // Public --------------------------------------------------------

   public Throwable getPayload()
   {
      return getCause();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
