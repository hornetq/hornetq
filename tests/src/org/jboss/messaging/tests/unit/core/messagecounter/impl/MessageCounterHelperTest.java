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

package org.jboss.messaging.tests.unit.core.messagecounter.impl;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.io.StringReader;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import junit.framework.TestCase;

import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterHelper;
import org.jboss.messaging.core.server.Queue;
import org.xml.sax.InputSource;
import org.xml.sax.helpers.DefaultHandler;

/**
 * A MessageCounterHelperTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 */
public class MessageCounterHelperTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testMessageCounterHistoryAsHTMLIsWellFormedIsWellFormed() throws Exception
   {
      Queue queue = createMock(Queue.class);
      MessageCounter counter1 = new MessageCounter(randomString(),
                                                  randomString(),
                                                  queue,
                                                  randomBoolean(),
                                                  randomBoolean(),
                                                  7);
      MessageCounter counter2 = new MessageCounter(randomString(),
                                                   randomString(),
                                                   queue,
                                                   randomBoolean(),
                                                   randomBoolean(),
                                                   7);
      MessageCounter[] counters = new MessageCounter[] { counter1, counter2 };

      replay(queue);

      String historyHTML = MessageCounterHelper.listMessageCounterHistoryAsHTML(counters);
      assertWellFormedXML(historyHTML);

      verify(queue);
   }

   public void testMessageCounterAsHTMLIsWellFormed() throws Exception
   {
      Queue queue1 = createMock(Queue.class);
      expect(queue1.getMessageCount()).andStubReturn(randomPositiveInt());
      MessageCounter counter1 = new MessageCounter(randomString(),
                                                  randomString(),
                                                  queue1,
                                                  randomBoolean(),
                                                  randomBoolean(),
                                                  7);
      Queue queue2 = createMock(Queue.class);
      expect(queue2.getMessageCount()).andStubReturn(randomPositiveInt());
      MessageCounter counter2 = new MessageCounter(randomString(),
                                                   randomString(),
                                                   queue2,
                                                   randomBoolean(),
                                                   randomBoolean(),
                                                   7);
      MessageCounter[] counters = new MessageCounter[] { counter1, counter2 };

      replay(queue1, queue2);

      String countersHTML = MessageCounterHelper.listMessageCounterAsHTML(counters);
      assertWellFormedXML(countersHTML);

      verify(queue1, queue2);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private static void assertWellFormedXML(String xmlASstring) throws Exception
   {
         SAXParserFactory spfactory = SAXParserFactory.newInstance();
         SAXParser saxParser = spfactory.newSAXParser();

         StringReader reader = new StringReader(xmlASstring);
         // if the xml is not well-formed, an exception is thrown
         saxParser.parse(new InputSource(reader), new DefaultHandler());
   }

   // Inner classes -------------------------------------------------

}
