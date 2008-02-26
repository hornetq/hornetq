package org.jboss.messaging.core.remoting.impl.wireformat;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.SESS_BINDINGQUERY_RESP;

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
   private final boolean exists;
   
   private final List<String> queueNames;
   
   public SessionBindingQueryResponseMessage(final boolean exists, final List<String> queueNames)
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
