/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.perf.framework.remoting;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class SimpleResult extends ResultSupport
{
   // Constants -----------------------------------------------------

   public static final long serialVersionUID = 357239732309853045L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SimpleResult()
   {
      super();
   }

   public SimpleResult(Request request)
   {
      super(request);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "EMPTY RESULT";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
