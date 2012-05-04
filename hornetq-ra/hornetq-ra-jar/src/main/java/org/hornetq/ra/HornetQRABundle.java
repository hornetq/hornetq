package org.hornetq.ra;


import org.hornetq.api.core.*;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

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

   @Message(id = 159001, value = "Error decoding password using codec instance", format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException errorDecodingPassword(@Cause Exception e);


}
