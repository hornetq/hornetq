package org.hornetq.tests.integration.stomp.v11;

import org.hornetq.tests.integration.stomp.util.ClientStompFrame;
import org.hornetq.tests.integration.stomp.util.StompClientConnection;
import org.hornetq.tests.integration.stomp.util.StompClientConnectionFactory;

/*
 * Some Stomp tests against server with persistence enabled are put here.
 */
public class ExtraStompTest extends StompTestBase2
{
   
   protected void setUp() throws Exception
   {
      persistenceEnabled = true;
      super.setUp();
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testSendAndReceive10() throws Exception
   {
      StompClientConnection connV11 = StompClientConnectionFactory.createClientConnection("1.0", hostname, port);

      try
      {
         String msg1 = "Hello World 1!";
         String msg2 = "Hello World 2!";
         
      connV11.connect(defUser, defPass);
      
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", String.valueOf(msg1.getBytes("UTF-8").length));
      frame.addHeader("persistent", "true");
      frame.setBody(msg1);
      
      connV11.sendFrame(frame);

      ClientStompFrame frame2 = connV11.createFrame("SEND");
      frame2.addHeader("destination", getQueuePrefix() + getQueueName());
      frame2.addHeader("content-length", String.valueOf(msg2.getBytes("UTF-8").length));
      frame2.addHeader("persistent", "true");
      frame2.setBody(msg2);
      
      connV11.sendFrame(frame2);
      
      ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      
      connV11.sendFrame(subFrame);
      
      frame = connV11.receiveFrame();
      
      System.out.println("received " + frame);
      
      assertEquals("MESSAGE", frame.getCommand());
      
      assertEquals("a-sub", frame.getHeader("subscription"));
      
      assertNotNull(frame.getHeader("message-id"));
      
      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));
      
      assertEquals(msg1, frame.getBody());
      
      frame = connV11.receiveFrame();
      
      System.out.println("received " + frame);      
      
      assertEquals("MESSAGE", frame.getCommand());
      
      assertEquals("a-sub", frame.getHeader("subscription"));
      
      assertNotNull(frame.getHeader("message-id"));
      
      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));
      
      assertEquals(msg2, frame.getBody());
      
      //unsub
      ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV11.sendFrame(unsubFrame);
      
      }
      finally
      {
         connV11.disconnect();
      }
   }

   public void testSendAndReceive11() throws Exception
   {
      StompClientConnection connV11 = StompClientConnectionFactory.createClientConnection("1.1", hostname, port);

      try
      {
         String msg1 = "Hello World 1!";
         String msg2 = "Hello World 2!";
         
      connV11.connect(defUser, defPass);
      
      ClientStompFrame frame = connV11.createFrame("SEND");
      frame.addHeader("destination", getQueuePrefix() + getQueueName());
      frame.addHeader("content-length", String.valueOf(msg1.getBytes("UTF-8").length));
      frame.addHeader("persistent", "true");
      frame.setBody(msg1);
      
      connV11.sendFrame(frame);

      ClientStompFrame frame2 = connV11.createFrame("SEND");
      frame2.addHeader("destination", getQueuePrefix() + getQueueName());
      frame2.addHeader("content-length", String.valueOf(msg2.getBytes("UTF-8").length));
      frame2.addHeader("persistent", "true");
      frame2.setBody(msg2);
      
      connV11.sendFrame(frame2);
      
      ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
      subFrame.addHeader("id", "a-sub");
      subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
      subFrame.addHeader("ack", "auto");
      
      connV11.sendFrame(subFrame);
      
      frame = connV11.receiveFrame();
      
      System.out.println("received " + frame);
      
      assertEquals("MESSAGE", frame.getCommand());
      
      assertEquals("a-sub", frame.getHeader("subscription"));
      
      assertNotNull(frame.getHeader("message-id"));
      
      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));
      
      assertEquals(msg1, frame.getBody());
      
      frame = connV11.receiveFrame();
      
      System.out.println("received " + frame);      
      
      assertEquals("MESSAGE", frame.getCommand());
      
      assertEquals("a-sub", frame.getHeader("subscription"));
      
      assertNotNull(frame.getHeader("message-id"));
      
      assertEquals(getQueuePrefix() + getQueueName(), frame.getHeader("destination"));
      
      assertEquals(msg2, frame.getBody());
      
      //unsub
      ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
      unsubFrame.addHeader("id", "a-sub");
      connV11.sendFrame(unsubFrame);
      
      }
      finally
      {
         connV11.disconnect();
      }
   }

   public void testNoGarbageAfterPersistentMessageV10() throws Exception
   {
      StompClientConnection connV11 = StompClientConnectionFactory
            .createClientConnection("1.0", hostname, port);
      try
      {
         connV11.connect(defUser, defPass);

         ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         connV11.sendFrame(subFrame);

         ClientStompFrame frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-length", "11");
         frame.addHeader("persistent", "true");
         frame.setBody("Hello World");

         connV11.sendFrame(frame);

         frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-length", "11");
         frame.addHeader("persistent", "true");
         frame.setBody("Hello World");

         connV11.sendFrame(frame);

         frame = connV11.receiveFrame(10000);
         
         System.out.println("received: " + frame);
         
         assertEquals("Hello World", frame.getBody());

         //if hornetq sends trailing garbage bytes, the second message
         //will not be normal
         frame = connV11.receiveFrame(10000);
         
         System.out.println("received: " + frame);
         
         assertEquals("Hello World", frame.getBody());

         //unsub
         ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         connV11.sendFrame(unsubFrame);
      }
      finally
      {
         connV11.disconnect();
      }
   }
   
   public void testNoGarbageOnPersistentRedeliveryV10() throws Exception
   {
      StompClientConnection connV11 = StompClientConnectionFactory
            .createClientConnection("1.0", hostname, port);
      try
      {
         connV11.connect(defUser, defPass);

         ClientStompFrame frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-length", "11");
         frame.addHeader("persistent", "true");
         frame.setBody("Hello World");

         connV11.sendFrame(frame);

         frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-length", "11");
         frame.addHeader("persistent", "true");
         frame.setBody("Hello World");

         connV11.sendFrame(frame);

         ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "client");

         connV11.sendFrame(subFrame);

         // receive but don't ack
         frame = connV11.receiveFrame(10000);
         frame = connV11.receiveFrame(10000);
         
         System.out.println("received: " + frame);

         //unsub
         ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         connV11.sendFrame(unsubFrame);

         subFrame = connV11.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         connV11.sendFrame(subFrame);
         
         frame = connV11.receiveFrame(10000);
         frame = connV11.receiveFrame(10000);
         
         //second receive will get problem if trailing bytes
         assertEquals("Hello World", frame.getBody());
         
         System.out.println("received again: " + frame);

         //unsub
         unsubFrame = connV11.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         connV11.sendFrame(unsubFrame);
      }
      finally
      {
         connV11.disconnect();
      }
   }

   public void testNoGarbageAfterPersistentMessageV11() throws Exception
   {
      StompClientConnection connV11 = StompClientConnectionFactory
            .createClientConnection("1.1", hostname, port);
      try
      {
         connV11.connect(defUser, defPass);

         ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         connV11.sendFrame(subFrame);

         ClientStompFrame frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-length", "11");
         frame.addHeader("persistent", "true");
         frame.setBody("Hello World");

         connV11.sendFrame(frame);

         frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-length", "11");
         frame.addHeader("persistent", "true");
         frame.setBody("Hello World");

         connV11.sendFrame(frame);

         frame = connV11.receiveFrame(10000);
         
         System.out.println("received: " + frame);
         
         assertEquals("Hello World", frame.getBody());

         //if hornetq sends trailing garbage bytes, the second message
         //will not be normal
         frame = connV11.receiveFrame(10000);
         
         System.out.println("received: " + frame);
         
         assertEquals("Hello World", frame.getBody());

         //unsub
         ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         connV11.sendFrame(unsubFrame);
      }
      finally
      {
         connV11.disconnect();
      }
   }
   
   public void testNoGarbageOnPersistentRedeliveryV11() throws Exception
   {
      StompClientConnection connV11 = StompClientConnectionFactory
            .createClientConnection("1.1", hostname, port);
      try
      {
         connV11.connect(defUser, defPass);

         ClientStompFrame frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-length", "11");
         frame.addHeader("persistent", "true");
         frame.setBody("Hello World");

         connV11.sendFrame(frame);

         frame = connV11.createFrame("SEND");
         frame.addHeader("destination", getQueuePrefix() + getQueueName());
         frame.addHeader("content-length", "11");
         frame.addHeader("persistent", "true");
         frame.setBody("Hello World");

         connV11.sendFrame(frame);

         ClientStompFrame subFrame = connV11.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "client");

         connV11.sendFrame(subFrame);

         // receive but don't ack
         frame = connV11.receiveFrame(10000);
         frame = connV11.receiveFrame(10000);
         
         System.out.println("received: " + frame);

         //unsub
         ClientStompFrame unsubFrame = connV11.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         connV11.sendFrame(unsubFrame);

         subFrame = connV11.createFrame("SUBSCRIBE");
         subFrame.addHeader("id", "a-sub");
         subFrame.addHeader("destination", getQueuePrefix() + getQueueName());
         subFrame.addHeader("ack", "auto");

         connV11.sendFrame(subFrame);
         
         frame = connV11.receiveFrame(10000);
         frame = connV11.receiveFrame(10000);
         
         //second receive will get problem if trailing bytes
         assertEquals("Hello World", frame.getBody());
         
         System.out.println("received again: " + frame);

         //unsub
         unsubFrame = connV11.createFrame("UNSUBSCRIBE");
         unsubFrame.addHeader("id", "a-sub");
         connV11.sendFrame(unsubFrame);
      }
      finally
      {
         connV11.disconnect();
      }
   }

}
