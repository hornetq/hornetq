/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.tools.jmx;

import org.jboss.tm.TxManager;

import javax.transaction.TransactionManager;


/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TransactionManagerJMXWrapper implements TransactionManagerJMXWrapperMBean
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private TransactionManager tm;

   // Constructors --------------------------------------------------

   public TransactionManagerJMXWrapper(TransactionManager tm)
   {
      this.tm = tm;
   }

   // TransactionManagerJMXWrapperMBean implementation --------------

   public TransactionManager getTransactionManager()
   {
      return tm;
   }

   public void start() throws Exception
   {
   }

   public void stop() throws Exception
   {
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
