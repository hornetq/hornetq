package org.hornetq.jms.client;


import org.hornetq.api.core.HornetQInvalidFilterExpressionException;
import org.hornetq.api.core.SimpleString;
import org.jboss.logging.Cause;
import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 12
 *
 * each message id must be 6 digits long starting with 10, the 3rd digit should be 9
 *
 * so 129000 to 129999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQJMSClientBundle
{
   HornetQJMSClientBundle BUNDLE = Messages.getBundle(HornetQJMSClientBundle.class);

   @Message(id = 129009, value =  "Invalid filter: {0}", format = Message.Format.MESSAGE_FORMAT)
   HornetQInvalidFilterExpressionException invalidFilter(@Cause Throwable e, SimpleString filter);
}
