/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.topic;

import javax.jms.MessageListener;
import javax.jms.Message;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>

 * $Id$
 */
public class ExampleListener implements MessageListener
{

   private Message message;


   public synchronized void onMessage(Message message)
   {
      this.message = message;
      notifyAll();
   }

   public synchronized Message getMessage()
   {
      return message;
   }


   protected synchronized void waitForMessage()
   {
      if (message != null)
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
