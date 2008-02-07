package org.jboss.messaging.core.remoting.wireformat;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.SESS_BINDINGQUERY_RESP;

import java.util.List;

/**
 * 
 * A SessionBindingQueryResponseMessage
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionBindingQueryResponseMessage extends AbstractPacket
{
   private boolean exists;
   
   private List<String> queueNames;
   
   public SessionBindingQueryResponseMessage(boolean exists, List<String> queueNames)
   {
      super(SESS_BINDINGQUERY_RESP);

      this.exists = exists;

      this.queueNames = queueNames;
   }

   public boolean isExists()
   {
      return exists;
   }

   public List<String> getQueueNames()
   {
      return this.queueNames;
   }

}
