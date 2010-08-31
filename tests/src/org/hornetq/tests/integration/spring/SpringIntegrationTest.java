package org.hornetq.tests.integration.spring;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SpringIntegrationTest extends TestCase
{
   public void testSpring() throws Exception
   {
      System.out.println("Creating bean factory...");
      ApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"spring-jms-beans.xml"});
      MessageSender sender = (MessageSender)context.getBean("MessageSender");
      System.out.println("Sending message...");
      sender.send("Hello world");
      Thread.sleep(100);
      Assert.assertEquals(ExampleListener.lastMessage, "Hello world");
   }
}
