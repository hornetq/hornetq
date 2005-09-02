
package org.jboss.test.messaging.jms.managed;

import javax.ejb.MessageDrivenBean;
import javax.ejb.MessageDrivenContext;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.jboss.logging.Logger;


public class MessageBean implements MessageDrivenBean, MessageListener
{
   private static Logger log = Logger.getLogger(MessageBean.class);
   
   private MessageDrivenContext ctx;
   
   public void setMessageDrivenContext(MessageDrivenContext mdc)
   {
      log.trace("setMessageDrivenContext messageBean");
      this.ctx = mdc;
   }
   
   public void ejbCreate()
   {
      log.trace("ejbCreate:created messageBean");
   }
   
   public void onMessage(Message inMessage)
   {
      
      TextMessage msg = (TextMessage)inMessage;
      
      log.trace("Received message:" + msg);

   }
   
   public void ejbRemove()
   {
      log.trace("ejbRemoved:removed messageBean");
   }
}
