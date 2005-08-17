/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.tools.jmx;

import org.jboss.tm.TxManager;

import javax.transaction.TransactionManager;

public class TransactionManagerJMXWrapper implements TransactionManagerJMXWrapperMBean
{

   private TransactionManager tm;


   public TransactionManagerJMXWrapper(TransactionManager tm)
   {
      this.tm = tm;
   }

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

}
