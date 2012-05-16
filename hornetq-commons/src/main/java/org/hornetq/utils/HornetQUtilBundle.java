package org.hornetq.utils;


import org.jboss.logging.Message;
import org.jboss.logging.MessageBundle;
import org.jboss.logging.Messages;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         3/12/12
 *
 * Logger Code 20
 *
 * each message id must be 6 digits long starting with 20, the 3rd digit should be 9
 *
 * so 209000 to 209999
 */
@MessageBundle(projectCode = "HQ")
public interface HornetQUtilBundle
{
   HornetQUtilBundle BUNDLE = Messages.getBundle(HornetQUtilBundle.class);

   @Message(id = 209001, value = "invalid property: {0}" , format = Message.Format.MESSAGE_FORMAT)
   org.hornetq.api.core.IllegalStateException invalidProperty(String part);
}
