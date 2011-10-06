package org.hornetq.tests.integration.spring;

import junit.framework.Assert;

import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.embedded.EmbeddedJMS;
import org.hornetq.tests.util.UnitTestCase;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SpringIntegrationTest extends UnitTestCase
{
   Logger log = Logger.getLogger(SpringIntegrationTest.class);
   
   protected void setUp() throws Exception
   {
      super.setUp();
      // Need to force GC as the connection on the spring needs to be cleared
      // otherwise the sprint thread may leak here
      forceGC();
   }
   
   public void testSpring() throws Exception
   {
      System.out.println("Creating bean factory...");
      ApplicationContext context = new ClassPathXmlApplicationContext(new String[] { "spring-jms-beans.xml" });
      try
      {
         MessageSender sender = (MessageSender)context.getBean("MessageSender");
         System.out.println("Sending message...");
         sender.send("Hello world");
         Thread.sleep(100);
         Assert.assertEquals(ExampleListener.lastMessage, "Hello world");
         ((HornetQConnectionFactory)sender.getConnectionFactory()).close();
      }
      finally
      {
         try
         {
            DefaultMessageListenerContainer container = (DefaultMessageListenerContainer)context.getBean("listenerContainer");
            container.stop();
         }
         catch (Throwable ignored)
         {
            ignored.printStackTrace();
         }
         try
         {
            EmbeddedJMS jms = (EmbeddedJMS)context.getBean("EmbeddedJms");
            jms.stop();
         }
         catch (Throwable ignored)
         {
            ignored.printStackTrace();
         }
      }

   }
}
