/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.remoting.wireformat;

import org.jboss.jms.tx.TransactionRequest;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SendTransactionMessage extends AbstractPacket
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final TransactionRequest transactionRequest;
   private final boolean checkForDuplicates;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SendTransactionMessage(TransactionRequest transactionRequest,
         boolean checkForDuplicates)
   {
      super(PacketType.MSG_SENDTRANSACTION);

      assert transactionRequest != null;

      this.transactionRequest = transactionRequest;
      this.checkForDuplicates = checkForDuplicates;
   }

   // Public --------------------------------------------------------

   public TransactionRequest getTransactionRequest()
   {
      return transactionRequest;
   }

   public boolean checkForDuplicates()
   {
      return checkForDuplicates;
   }

   @Override
   public String toString()
   {
      return getParentString() + ", transactionRequest=" + transactionRequest
            + ", checkForDuplicates=" + checkForDuplicates + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
