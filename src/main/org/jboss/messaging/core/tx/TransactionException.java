/*
 * JBoss, the OpenSource J2EE webOS
 * 
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.tx;

public class TransactionException extends Exception
{
   private static final long serialVersionUID = 2461082362655828741L;

   public TransactionException(String msg)
   {
      super(msg);
   }
   
   public TransactionException(String msg, Throwable t)
   {
      super(msg, t);
   }
}
