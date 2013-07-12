/**
 *
 */
package org.hornetq.jms.client;

import javax.jms.IllegalStateRuntimeException;
import javax.jms.InvalidClientIDException;
import javax.jms.InvalidClientIDRuntimeException;
import javax.jms.InvalidDestinationException;
import javax.jms.InvalidDestinationRuntimeException;
import javax.jms.InvalidSelectorException;
import javax.jms.InvalidSelectorRuntimeException;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.JMSSecurityException;
import javax.jms.JMSSecurityRuntimeException;
import javax.jms.MessageFormatException;
import javax.jms.MessageFormatRuntimeException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageNotWriteableRuntimeException;
import javax.jms.ResourceAllocationException;
import javax.jms.ResourceAllocationRuntimeException;
import javax.jms.TransactionInProgressException;
import javax.jms.TransactionInProgressRuntimeException;
import javax.jms.TransactionRolledBackException;
import javax.jms.TransactionRolledBackRuntimeException;

/**
 *
 */
public final class JmsExceptionUtils
{
   private JmsExceptionUtils()
   {
      // utility class
   }

   /**
    * Converts instances of sub-classes of {@link JMSException} into the corresponding sub-class of
    * {@link JMSRuntimeException}.
    * @param e
    * @return
    */
   public static JMSRuntimeException convertToRuntimeException(JMSException e)
   {
      if (e instanceof javax.jms.IllegalStateException)
      {
         return new IllegalStateRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof InvalidClientIDException)
      {
         return new InvalidClientIDRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof InvalidDestinationException)
      {
         return new InvalidDestinationRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof InvalidSelectorException)
      {
         return new InvalidSelectorRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof JMSSecurityException)
      {
         return new JMSSecurityRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof MessageFormatException)
      {
         return new MessageFormatRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof MessageNotWriteableException)
      {
         return new MessageNotWriteableRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof ResourceAllocationException)
      {
         return new ResourceAllocationRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof TransactionInProgressException)
      {
         return new TransactionInProgressRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      if (e instanceof TransactionRolledBackException)
      {
         return new TransactionRolledBackRuntimeException(e.getMessage(), e.getErrorCode(), e);
      }
      return new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
   }
}
