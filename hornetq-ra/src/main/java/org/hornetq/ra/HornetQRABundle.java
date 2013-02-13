package org.hornetq.ra;


import org.hornetq.api.core.HornetQIllegalStateException;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

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
   HornetQRABundle BUNDLE = Messages.getBundle(HornetQRABundle.class);

   @Message(id = 159000, value = "Error decoding password using codec instance", format = Message.Format.MESSAGE_FORMAT)
   HornetQIllegalStateException errorDecodingPassword(@Cause Exception e);

   @Message(id = 159001, value = "MDB cannot be deployed as it has no Activation Spec. Please provide an Activation!", format = Message.Format.MESSAGE_FORMAT)
   NotSupportedException noActivationSpec();

   @Message(id = 159002, value = "Please provide a destination for the MDB", format = Message.Format.MESSAGE_FORMAT)
   IllegalArgumentException noDestinationName();
}
