/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.integration.core.remoting.mina;


/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>.
 * @version <tt>$Revision$</tt>
 */
public abstract class TestSupport
{
   // Constants -----------------------------------------------------

   public static final int MANY_MESSAGES = 50000;

   public static final int KEEP_ALIVE_INTERVAL = 2000; // in seconds

   public static final int KEEP_ALIVE_TIMEOUT = 1000; // in seconds

   public static final long REQRES_TIMEOUT = 2; // in seconds

   public static final int PORT = 9090;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static String reverse(String text)
   {
      // Reverse text
      StringBuffer buf = new StringBuffer(text.length());
      for (int i = text.length() - 1; i >= 0; i--)
      {
         buf.append(text.charAt(i));
      }
      return buf.toString();
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
