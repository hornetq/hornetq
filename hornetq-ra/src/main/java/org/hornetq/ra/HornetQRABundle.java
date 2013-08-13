package org.hornetq.ra;


import org.hornetq.api.core.HornetQIllegalStateException;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.resource.NotSupportedException;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 15
 *
 * each message id must be 6 digits long starting with 15, the 3rd digit should be 9
 *
 * so 159000 to 159999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQRABundle
{
   /** Error message for strict behaviour */
   String ISE = "This method is not applicable inside the application server. See the JEE spec, e.g. JEE 7 Section 6.7";

   HornetQRABundle BUNDLE = Messages.getBundle(HornetQRABundle.class);

   @Message(id = 159000, value = "Error decoding password using codec instance", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException errorDecodingPassword(@Cause Exception e);

   @Message(id = 159001, value = "MDB cannot be deployed as it has no Activation Spec. Please provide an Activation!", format = Message.Format.MESSAGE_FORMAT)
   NotSupportedException noActivationSpec();

   @Message(id = 159002, value = "Please provide a destination for the MDB", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noDestinationName();

   @Message(id = 159003, value = ISE, format = Message.Format.MESSAGE_FORMAT)
   JMSRuntimeException illegalJEEMethod();

   @Message(id = 159004, value = "Invalid Session Mode SESSION_TRANSACTED", format = Message.Format.MESSAGE_FORMAT)
   JMSRuntimeException invalidSessionTransactedModeRuntime();

   @Message(id = 159005, value = "Invalid Session Mode CLIENT_ACKNOWLEDGE", format = Message.Format.MESSAGE_FORMAT)
   JMSRuntimeException invalidClientAcknowledgeModeRuntime();

   @Message(id = 159006, value = "Invalid Session Mode {0}", format = Message.Format.MESSAGE_FORMAT)
   JMSRuntimeException invalidAcknowledgeMode(int sessionMode);

   @Message(id = 159007, value = "Invalid Session Mode SESSION_TRANSACTED", format = Message.Format.MESSAGE_FORMAT)
   JMSException invalidSessionTransactedMode();

   @Message(id = 159008, value = "Invalid Session Mode CLIENT_ACKNOWLEDGE", format = Message.Format.MESSAGE_FORMAT)
   JMSException invalidClientAcknowledgeMode();
}
