/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.tests.unit.jms.client;

import org.easymock.IArgumentMatcher;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossConnectionTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testFoo()
   {      
   }
//
//   public void testStart() throws Exception
//   {      
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      ClientSession sess1 = createStrictMock(ClientSession.class);
//      ClientSession sess2 = createStrictMock(ClientSession.class);
//      ClientSession sess3 = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess1);
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess2);
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess3);
//      
//      sess1.addFailureListener(EasyMock.isA(FailureListener.class));
//      sess2.addFailureListener(EasyMock.isA(FailureListener.class));
//      sess3.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      sess1.start();
//      sess2.start();
//      sess3.start();
//      
//      replay(sf, sess1, sess2, sess3);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, -1, sf);
//      
//      assertNotNull(connection.getUID());
//      
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//      connection.start();
//
//      verify(sf, sess1, sess2, sess3);      
//   }
//
//   public void testStartThrowsException() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      ClientSession sess1 = createStrictMock(ClientSession.class);
//           
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess1);     
//      
//      sess1.addFailureListener(EasyMock.isA(FailureListener.class));
//
//      sess1.start();
//      expectLastCall().andThrow(new MessagingException());
//
//      replay(sf, sess1);
//
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, -1, sf);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//      try
//      {
//         connection.start();
//         fail("should throw a JMSException");
//      } catch(JMSException e)
//      {
//      }
//
//      verify(sf, sess1);
//   }
//   
//   public void testStop() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      ClientSession sess1 = createStrictMock(ClientSession.class);
//      ClientSession sess2 = createStrictMock(ClientSession.class);
//      ClientSession sess3 = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess1);
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess2);
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess3);
//      
//      sess1.addFailureListener(EasyMock.isA(FailureListener.class));
//      sess2.addFailureListener(EasyMock.isA(FailureListener.class));
//      sess3.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      sess1.stop();
//      sess2.stop();
//      sess3.stop();
//      
//      replay(sf, sess1, sess2, sess3);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, -1, sf);
//      
//      assertNotNull(connection.getUID());
//      
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//      connection.stop();
//
//      verify(sf, sess1, sess2, sess3);      
//   }
//
//   public void testStopThrowsException() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      ClientSession sess1 = createStrictMock(ClientSession.class);
//           
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess1);     
//      
//      sess1.addFailureListener(EasyMock.isA(FailureListener.class));
//
//      sess1.stop();
//      expectLastCall().andThrow(new MessagingException());
//
//      replay(sf, sess1);
//
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, -1, sf);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//      try
//      {
//         connection.stop();
//         fail("should throw a JMSException");
//      } catch(JMSException e)
//      {
//      }
//
//      verify(sf, sess1);
//   }
//   
//   public void testClose() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      ClientSession sess1 = createStrictMock(ClientSession.class);
//      ClientSession sess2 = createStrictMock(ClientSession.class);
//      ClientSession sess3 = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess1);
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess2);
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess3);
//      
//      sess1.addFailureListener(EasyMock.isA(FailureListener.class));
//      sess2.addFailureListener(EasyMock.isA(FailureListener.class));
//      sess3.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      sess1.close();
//      sess2.close();
//      sess3.close();
//      
//      replay(sf, sess1, sess2, sess3);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//            JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, -1, sf);
//      
//      assertNotNull(connection.getUID());
//      
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//      connection.close();
//
//      verify(sf, sess1, sess2, sess3);      
//   }
//   
//   public void testCloseThrowsException() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      ClientSession sess1 = createStrictMock(ClientSession.class);
//           
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false, 0)).andReturn(sess1);     
//      
//      sess1.addFailureListener(EasyMock.isA(FailureListener.class));
//
//      sess1.close();
//      expectLastCall().andThrow(new MessagingException());
//
//      replay(sf, sess1);
//
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//      try
//      {
//         connection.close();
//         fail("should throw a JMSException");
//      } catch(JMSException e)
//      {
//      }
//
//      verify(sf, sess1);
//   }
//   
//   public void testUsingClosedConnection() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      
//      connection.close();
//
//      try
//      {
//         connection.getClientID();
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createSession(false, 1);
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.setClientID("123");
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.getMetaData();
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.getExceptionListener();
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.setExceptionListener(new ExceptionListener() {
//            public void onException(JMSException e)
//            {              
//            }
//         });
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.setExceptionListener(new ExceptionListener() {
//            public void onException(JMSException e)
//            {              
//            }
//         });
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.start();
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.stop();
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createConnectionConsumer((Destination)null, null, null, 23);
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createDurableConnectionConsumer((Topic)null, null, null, null, 23);
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createQueueSession(false, 1);
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createConnectionConsumer((Queue)null, null, null, 23);
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createTopicSession(false, 1);
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createConnectionConsumer((Topic)null, null, null, 23);
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createXASession();
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createXAQueueSession();
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//      try
//      {
//         connection.createXATopicSession();
//         fail("should throw a JMSException");
//      }
//      catch (JMSException e)
//      {
//      };
//   }
//
//   public void testGetClientID() throws Exception
//   {
//      String clientID = randomString();
//      
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, clientID, -1, sf);
//      
//      assertEquals(clientID, connection.getClientID());
//   }
//
//   public void testSetClientID() throws Exception
//   {
//      String clientID = randomString();
//      
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      
//      connection.setClientID(clientID);
//      
//      assertEquals(clientID, connection.getClientID());
//   }
//
//   public void testSetClientIDFailsIfClientIDAlreadyExists() throws Exception
//   {
//      String clientID = randomString();
//      
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      
//      connection.setClientID(clientID);
//      
//      assertEquals(clientID, connection.getClientID());
//
//      try
//      {
//         connection.setClientID(randomString());
//         fail("should throw a JMS Exception");
//      } catch (JMSException e)
//      {
//      }
//   }
//
//   public void testSetClientIDFailsIfConnectionAlreadyUsed() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      
//      connection.start();
//      
//      try
//      {
//         connection.setClientID(randomString());
//         fail("should throw a JMS Exception");
//      } catch (JMSException e)
//      {
//      }
//   }
//   
//   public void testGetMetaData() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      
//      ConnectionMetaData data = connection.getMetaData();
//      
//      assertNotNull(data);
//   }
//   
//   public void testSetGetExceptionListener() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      
//      ExceptionListener listener = createStrictMock(ExceptionListener.class);
//      
//      assertNull(connection.getExceptionListener());
//           
//      connection.setExceptionListener(listener);
//      
//      assertEquals(listener, connection.getExceptionListener());
//      
//      connection.setExceptionListener(null);
//      
//      assertNull(connection.getExceptionListener());
//  }
//   
//   public void testCreateConnectionConsumerFromDestination() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      Destination destination = createStrictMock(Destination.class);
//      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);
//
//      ConnectionConsumer connConsumer = connection.createConnectionConsumer(destination, null, sessionPool, 10);
//      assertNull(connConsumer); 
//   }
//   
//   public void testCreateConnectionConsumerFromQueue() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      Queue queue = createStrictMock(Queue.class);
//      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);
//
//      ConnectionConsumer connConsumer = connection.createConnectionConsumer(queue, null, sessionPool, 10);
//      assertNull(connConsumer); 
//   }
//   
//   public void testCreateConnectionConsumerFromTopic() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
//      Topic topic = createStrictMock(Topic.class);
//      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);
//      
//
//      ConnectionConsumer connConsumer = connection.createConnectionConsumer(topic, null, sessionPool, 10);
//      assertNull(connConsumer); 
//   }
//      
//   public void testCreateDurableConnectionConsumerFromTopic() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
//      Topic topic = createStrictMock(Topic.class);
//      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);
//      
//
//      ConnectionConsumer connConsumer = connection.createDurableConnectionConsumer(topic, null, null, sessionPool, 10);
//      assertNull(connConsumer); 
//   }
//   
//   public void testCreateDurableConnectionConsumerFromQueueConnection() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      Topic topic = createStrictMock(Topic.class);
//      ServerSessionPool sessionPool = createStrictMock(ServerSessionPool.class);
//
//      try
//      {
//         connection.createDurableConnectionConsumer(topic, RandomUtil.randomString(), null, sessionPool, 10);
//         connection.setClientID(randomString());
//         fail("should throw a JMS Exception");
//      } catch (JMSException e)
//      {
//      }
//   }
//   
//   public void testCreateSessionThrowsException() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false)).andThrow(new MessagingException());
//      
//      replay(sf);
//  
//      try
//      {
//         connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//         fail("should throw a JMSException");
//      } catch(JMSException e)
//      {
//      }
//
//      verify(sf);
//   }
//   
//   public void testCreateTransactedQueueSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, false, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      QueueSession session = connection.createQueueSession(true, 0);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//
//   public void testCreateAutoAckQueueSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateDupsOKQueueSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      QueueSession session = connection.createQueueSession(false, Session.DUPS_OK_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateClientAckQueueSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      QueueSession session = connection.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateQueueSessionWithInvalidAckMode() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      replay(sf, clientSession);
//
//      
//      try 
//      {
//         connection.createQueueSession(false, 12345);
//         fail("must throw a IllegalArgumentException");
//      } catch (IllegalArgumentException e)
//      {         
//      }
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateTransactedTopicSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, false, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      TopicSession session = connection.createTopicSession(true, 0);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//
//   public void testCreateAutoAckTopicSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateDupsOKTopicSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      TopicSession session = connection.createTopicSession(false, Session.DUPS_OK_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateClientAckTopicSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      TopicSession session = connection.createTopicSession(false, Session.CLIENT_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateTopicSessionWithInvalidAckMode() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      replay(sf, clientSession);
//     
//      try 
//      {
//         connection.createTopicSession(false, 12345);
//         fail("must throw a IllegalArgumentException");
//      } catch (IllegalArgumentException e)
//      {         
//      }
//
//      verify(sf, clientSession);
//   }
//         
//   
//   
//   public void testCreateTransactedSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, false, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      Session session = connection.createSession(true, 0);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//
//   public void testCreateAutoAckSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateDupsOKSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateClientAckSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateSessionWithInvalidAckMode() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      replay(sf, clientSession);
//     
//      try 
//      {
//         connection.createSession(false, 12345);
//         fail("must throw a IllegalArgumentException");
//      } catch (IllegalArgumentException e)
//      {         
//      }
//
//      verify(sf, clientSession);
//   }
//   
//   
//   public void testCreateXASession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, true, false, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      XASession session = connection.createXASession();
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateXAQueueSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, true, false, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      XAQueueSession session = connection.createXAQueueSession();
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateXATopicSession() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, true, false, false, false)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      XATopicSession session = connection.createXATopicSession();
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   // here
//   
//   public void testCreateSessionThrowsExceptionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//
//      EasyMock.expect(sf.createSession(null, null, false, true, true, true)).andThrow(new MessagingException());
//      
//      replay(sf);
//  
//      try
//      {
//         connection.createSession(false, Session.AUTO_ACKNOWLEDGE, true);
//         fail("should throw a JMSException");
//      } catch(JMSException e)
//      {
//      }
//
//      verify(sf);
//   }
//   
//   public void testCreateTransactedQueueSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, false, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      QueueSession session = connection.createQueueSession(true, 0, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//
//   public void testCreateAutoAckQueueSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateDupsOKQueueSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      QueueSession session = connection.createQueueSession(false, Session.DUPS_OK_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateClientAckQueueSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      QueueSession session = connection.createQueueSession(false, Session.CLIENT_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateQueueSessionWithInvalidAckModeCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_QUEUE_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      replay(sf, clientSession);
//
//      
//      try 
//      {
//         connection.createQueueSession(false, 12345, true);
//         fail("must throw a IllegalArgumentException");
//      } catch (IllegalArgumentException e)
//      {         
//      }
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateTransactedTopicSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, false, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      TopicSession session = connection.createTopicSession(true, 0, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//
//   public void testCreateAutoAckTopicSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateDupsOKTopicSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      TopicSession session = connection.createTopicSession(false, Session.DUPS_OK_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateClientAckTopicSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      TopicSession session = connection.createTopicSession(false, Session.CLIENT_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateTopicSessionWithInvalidAckModeCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_TOPIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      replay(sf, clientSession);
//     
//      try 
//      {
//         connection.createTopicSession(false, 12345, true);
//         fail("must throw a IllegalArgumentException");
//      } catch (IllegalArgumentException e)
//      {         
//      }
//
//      verify(sf, clientSession);
//   }
//         
//   
//   
//   public void testCreateTransactedSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, false, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      Session session = connection.createSession(true, 0, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//
//   public void testCreateAutoAckSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, -1, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateDupsOKSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, true, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateClientAckSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, false, true, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE, true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateSessionWithInvalidAckModeCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      replay(sf, clientSession);
//     
//      try 
//      {
//         connection.createSession(false, 12345, true);
//         fail("must throw a IllegalArgumentException");
//      } catch (IllegalArgumentException e)
//      {         
//      }
//
//      verify(sf, clientSession);
//   }
//   
//   
//   public void testCreateXASessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, true, false, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      XASession session = connection.createXASession(true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateXAQueueSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, true, false, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      XAQueueSession session = connection.createXAQueueSession(true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
//   
//   public void testCreateXATopicSessionCacheProducers() throws Exception
//   {
//      ClientSessionFactory sf = createStrictMock(ClientSessionFactory.class);
//      
//      JBossConnection connection = new JBossConnection(null, null,
//               JBossConnection.TYPE_GENERIC_CONNECTION, null, 100, sf);
//      ClientSession clientSession = createStrictMock(ClientSession.class);
//      
//      EasyMock.expect(sf.createSession(null, null, true, false, false, true)).andReturn(clientSession);
//      clientSession.addFailureListener(EasyMock.isA(FailureListener.class));
//      
//      replay(sf, clientSession);
//
//      XATopicSession session = connection.createXATopicSession(true);
//      assertNotNull(session);
//
//      verify(sf, clientSession);
//   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   class FailureListenerMatcher implements IArgumentMatcher
   {
      FailureListener listener = null;
      public boolean matches(Object o)
      {
         listener = (FailureListener) o;
         return true;
      }

      public void appendTo(StringBuffer stringBuffer)
      {
         //we dont need this
      }
   }
}
