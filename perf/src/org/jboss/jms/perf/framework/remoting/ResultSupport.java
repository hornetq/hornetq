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
public class ResultSupport implements Result
{
   // Constants -----------------------------------------------------

   
   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected Request request;

   // Constructors --------------------------------------------------

   public ResultSupport()
   {
   }

   public ResultSupport(Request request)
   {
      this.request = request;
   }

   // Result support ------------------------------------------------

   public Request getRequest()
   {
      return request;
   }

   public void setRequest(Request request)
   {
      this.request = request;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
