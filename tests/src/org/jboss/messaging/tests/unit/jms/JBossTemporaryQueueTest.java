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

package org.jboss.messaging.tests.unit.jms;

import static org.easymock.EasyMock.isA;
import static org.easymock.classextension.EasyMock.createStrictMock;
import static org.easymock.classextension.EasyMock.replay;
import static org.easymock.classextension.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import org.jboss.messaging.jms.JBossTemporaryQueue;
import org.jboss.messaging.jms.client.JBossSession;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JBossTemporaryQueueTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testIsTemporary() throws Exception
   {
      String queueName = randomString();
      JBossSession session = createStrictMock(JBossSession.class);
      replay(session);

      JBossTemporaryQueue tempQueue = new JBossTemporaryQueue(session,
            queueName);
      assertEquals(true, tempQueue.isTemporary());

      verify(session);
   }

   public void testDelete() throws Exception
   {
      String queueName = randomString();
      JBossSession session = createStrictMock(JBossSession.class);
      session.deleteTemporaryQueue(isA(JBossTemporaryQueue.class));
      replay(session);

      JBossTemporaryQueue tempQueue = new JBossTemporaryQueue(session,
            queueName);
      tempQueue.delete();

      verify(session);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
