/*
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tests.unit.jms;

import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.ArrayList;

import javax.jms.Session;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionBindingQueryResponseMessage;
import org.jboss.messaging.jms.JBossTemporaryTopic;
import org.jboss.messaging.jms.client.JBossConnection;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.util.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossTemporaryTopicTest extends TestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testIsTemporary() throws Exception
   {
      String topicName = randomString();

      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);
      JBossTemporaryTopic tempTopic = new JBossTemporaryTopic(session,
            topicName);
      assertEquals(true, tempTopic.isTemporary());

      verify(clientConn, clientSession);
   }

   public void testDelete() throws Exception
   {
      String topicName = randomString();
      SimpleString topicAddress = new SimpleString(
            JBossTemporaryTopic.JMS_TEMP_TOPIC_ADDRESS_PREFIX + topicName);

      ClientConnection clientConn = createStrictMock(ClientConnection.class);
      ClientSession clientSession = createStrictMock(ClientSession.class);
      SessionBindingQueryResponseMessage resp = new SessionBindingQueryResponseMessage(
            true, new ArrayList<SimpleString>());
      expect(clientSession.bindingQuery(topicAddress)).andReturn(resp);
      clientSession.removeDestination(topicAddress, true);

      replay(clientConn, clientSession);

      JBossConnection connection = new JBossConnection(clientConn,
            JBossConnection.TYPE_TOPIC_CONNECTION, null, -1);
      JBossSession session = new JBossSession(connection, false, false,
            Session.AUTO_ACKNOWLEDGE, clientSession,
            JBossSession.TYPE_TOPIC_SESSION);
      JBossTemporaryTopic tempTopic = new JBossTemporaryTopic(session,
            topicName);
      tempTopic.delete();

      verify(clientConn, clientSession);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
