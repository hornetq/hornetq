package org.hornetq.tests.integration.spring;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.hornetq.utils.ReusableLatch;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class ExampleListener implements MessageListener
{
   public static String lastMessage = null;
   
   public static ReusableLatch latch = new ReusableLatch();

   public void onMessage(Message message)
   {
      try
      {
         lastMessage = ((TextMessage)message).getText();
      }
      catch (JMSException e)
      {
         throw new RuntimeException(e);
      }
      System.out.println("MESSAGE RECEIVED: " + lastMessage);
      latch.countDown();
   }
}
