/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.topiccluster;

import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.ArrayList;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @version <tt>$Revision: 536 $</tt>

 * $Id: ExampleListener.java 536 2005-12-22 00:28:39 -0600 (Thu, 22 Dec 2005) ovidiu $
 */
public class ExampleListener implements MessageListener
{

   public ExampleListener(String serverIP)
   {
       this.serverIP = serverIP;
   }

   String serverIP;
   ArrayList messages = new ArrayList();

   public synchronized void onMessage(Message message)
   {
      System.out.println("receiving message on server " + serverIP);
      this.messages.add(message);
      notifyAll();
   }

   public synchronized Message[] getMessages()
   {
      return (Message[])messages.toArray(new TextMessage[messages.size()]);
   }


   protected synchronized void waitForMessage(int numberOfExpectedMessages)
   {
      if (messages.size()==numberOfExpectedMessages)
      {
         return;
      }

      try
      {
         wait(5000);
      }
      catch(InterruptedException e)
      {
         // OK
      }
   }
}
