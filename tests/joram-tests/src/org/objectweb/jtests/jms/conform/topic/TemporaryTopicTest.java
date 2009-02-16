/*
 * JORAM: Java(TM) Open Reliable Asynchronous Messaging
 * Copyright (C) 2002 INRIA
 * Contact: joram-team@objectweb.org
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 * 
 * Initial developer(s): Jeff Mesnil (jmesnil@gmail.com)
 * Contributor(s): ______________________________________.
 */

package org.objectweb.jtests.jms.conform.topic;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.jms.TopicSubscriber;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.objectweb.jtests.jms.framework.PubSubTestCase;
import org.objectweb.jtests.jms.framework.TestConfig;

/**
 * Test the <code>javax.jms.TemporaryTopic</code> features.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: TemporaryTopicTest.java,v 1.1 2007/03/29 04:28:34 starksm Exp $
 */
public class TemporaryTopicTest extends PubSubTestCase
{

   private TemporaryTopic tempTopic;

   private TopicSubscriber tempSubscriber;

   /**
    * Test a TemporaryTopic
    */
   public void testTemporaryTopic()
   {
      try
      {
         // we stop both publisher and subscriber connections
         publisherConnection.stop();
         subscriberConnection.stop();
         // we create a temporary topic to receive messages
         tempTopic = subscriberSession.createTemporaryTopic();
         // we recreate the sender because it has been 
         // already created with another Destination as parameter
         publisher = publisherSession.createPublisher(tempTopic);
         // we create a temporary subscriber on the temporary topic
         tempSubscriber = subscriberSession.createSubscriber(tempTopic);
         subscriberConnection.start();
         publisherConnection.start();

         TextMessage message = publisherSession.createTextMessage();
         message.setText("testTemporaryTopic");
         publisher.publish(message);

         Message m = tempSubscriber.receive(TestConfig.TIMEOUT);
         assertTrue(m instanceof TextMessage);
         TextMessage msg = (TextMessage) m;
         assertEquals("testTemporaryTopic", msg.getText());
      }
      catch (JMSException e)
      {
         fail(e);
      }
   }

   /** 
    * Method to use this class in a Test suite
    */
   public static Test suite()
   {
      return new TestSuite(TemporaryTopicTest.class);
   }

   public TemporaryTopicTest(String name)
   {
      super(name);
   }
}
