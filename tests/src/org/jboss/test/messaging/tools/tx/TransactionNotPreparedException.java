/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.tx;

/**
 * Indicates that a transaction expected to be in the prepared state 
 * is actually not prepared.
 *
 * @author <a href="mailto:reverbel@ime.usp.br">Francisco Reverbel</a>
 * @version $Revision$ 
 */
class TransactionNotPreparedException extends Exception 
{
   public TransactionNotPreparedException()
   {
   }

   public TransactionNotPreparedException(String msg)
   {
      super(msg);
   }
}
