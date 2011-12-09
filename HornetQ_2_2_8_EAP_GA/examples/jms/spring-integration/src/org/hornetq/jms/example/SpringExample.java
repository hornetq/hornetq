package org.hornetq.jms.example;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SpringExample
{
   public static void main(String[] args) throws Exception
   {
      System.out.println("Creating bean factory...");
      ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"spring-jms-beans.xml"});
      MessageSender sender = (MessageSender)context.getBean("MessageSender");
      System.out.println("Sending message...");
      sender.send("Hello world");
      Thread.sleep(100);
      context.destroy();
   }
}
