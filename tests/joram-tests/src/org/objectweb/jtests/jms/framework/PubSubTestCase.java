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

package org.objectweb.jtests.jms.framework;

import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

import org.jboss.util.NestedRuntimeException;

/**
 * Creates convenient JMS Publish/Subscribe objects which can be needed for tests.
 * <br />
 * This class defines the setUp and tearDown methods so
 * that JMS administrated objects and  other "ready to use" Pub/Sub objects (that is to say topics,
 * sessions, publishers and subscribers) are available conveniently for the test cases.
 * <br />
 * Classes which want that convenience should extend <code>PubSubTestCase</code> instead of 
 * <code>JMSTestCase</code>.
 *
 * @author Jeff Mesnil (jmesnil@gmail.com)
 * @version $Id: PubSubTestCase.java,v 1.2 2007/06/19 23:32:35 csuconic Exp $
 */
public abstract class PubSubTestCase extends JMSTestCase
{

   private InitialContext ctx;

   private static final String TCF_NAME = "testTCF";

   private static final String TOPIC_NAME = "testJoramTopic";

   /**
    * Topic used by a publisher
    */
   protected Topic publisherTopic;

   /**
    * Publisher on queue
    */
   protected TopicPublisher publisher;

   /**
    * TopicConnectionFactory of the publisher
    */
   protected TopicConnectionFactory publisherTCF;

   /**
    * TopicConnection of the publisher
    */
   protected TopicConnection publisherConnection;

   /**
    * TopicSession of the publisher (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected TopicSession publisherSession;

   /**
    * Topic used by a subscriber
    */
   protected Topic subscriberTopic;

   /**
    * Subscriber on queue
    */
   protected TopicSubscriber subscriber;

   /**
    * TopicConnectionFactory of the subscriber
    */
   protected TopicConnectionFactory subscriberTCF;

   /**
    * TopicConnection of the subscriber
    */
   protected TopicConnection subscriberConnection;

   /**
    * TopicSession of the subscriber (non transacted, AUTO_ACKNOWLEDGE)
    */
   protected TopicSession subscriberSession;

   /**
    * Create all administrated objects connections and sessions ready to use for tests.
    * <br />
    * Start connections.
    */
   protected void setUp() throws Exception
   {
      super.setUp();
      
      try
      {
         // ...and creates administrated objects and binds them
         admin.createTopicConnectionFactory(TCF_NAME);
         admin.createTopic(TOPIC_NAME);

         // end of admin step, start of JMS client step
         ctx = admin.createInitialContext();

         publisherTCF = (TopicConnectionFactory) ctx.lookup(TCF_NAME);
         publisherTopic = (Topic) ctx.lookup(TOPIC_NAME);
         publisherConnection = publisherTCF.createTopicConnection();
         publisherConnection.setClientID("publisherConnection");
         publisherSession = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         publisher = publisherSession.createPublisher(publisherTopic);

         subscriberTCF = (TopicConnectionFactory) ctx.lookup(TCF_NAME);
         subscriberTopic = (Topic) ctx.lookup(TOPIC_NAME);
         subscriberConnection = subscriberTCF.createTopicConnection();
         subscriberConnection.setClientID("subscriberConnection");
         subscriberSession = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
         subscriber = subscriberSession.createSubscriber(subscriberTopic);

         publisherConnection.start();
         subscriberConnection.start();
         //end of client step
      }
      catch (Exception e)
      {
         throw new NestedRuntimeException(e);
      }
   }

   /**
    *  Close connections and delete administrated objects
    */
   protected void tearDown() throws Exception
   {
      try
      {
         publisherConnection.close();
         subscriberConnection.close();

         admin.deleteTopicConnectionFactory(TCF_NAME);
         admin.deleteTopic(TOPIC_NAME);
      }
      catch (Exception ignored)
      {
      }
      finally
      {
         publisherTopic = null;
         publisher = null;
         publisherTCF = null;
         publisherSession = null;
         publisherConnection = null;

         subscriberTopic = null;
         subscriber = null;
         subscriberTCF = null;
         subscriberSession = null;
         subscriberConnection = null;
      }
      
      super.tearDown();
   }

   public PubSubTestCase(String name)
   {
      super(name);
   }
}
