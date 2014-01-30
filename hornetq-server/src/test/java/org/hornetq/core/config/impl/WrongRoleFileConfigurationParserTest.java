/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.core.config.impl;

import java.io.ByteArrayInputStream;

import org.hornetq.core.deployers.impl.FileConfigurationParser;
import org.hornetq.tests.logging.AssertionLoggerHandler;
import org.hornetq.tests.util.UnitTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A FileConfigurationParserTest
 *
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 */
public class WrongRoleFileConfigurationParserTest extends UnitTestCase
{
   @BeforeClass
   public static void prepareLogger()
   {
      AssertionLoggerHandler.startCapture();
   }

   @Test
   public void testParsingDefaultServerConfig() throws Exception
   {
      FileConfigurationParser parser = new FileConfigurationParser();
      ByteArrayInputStream input = new ByteArrayInputStream(configuration.getBytes("UTF-8"));
      parser.parseMainConfig(input);

      // Using the code only because I don't want a test failing just for someone editing Log text
      AssertionLoggerHandler.findText("HQ222177");
      AssertionLoggerHandler.findText("HQ222177");
   }

   @AfterClass
   public static void clearLogger()
   {
      AssertionLoggerHandler.stopCapture();
   }

   private static final String configuration =
      "<configuration xmlns=\"urn:hornetq\"\n" +
         "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
         "xsi:schemaLocation=\"urn:hornetq /schema/hornetq-configuration.xsd\">\n" +
         "<name>HornetQ.main.config</name>" + "\n" +
         "<backup-group-name>anodegroupname</backup-group-name>" + "\n" +
         "<log-delegate-factory-class-name>org.hornetq.integration.logging.Log4jLogDelegateFactory</log-delegate-factory-class-name>" + "\n" +
         "<bindings-directory>${jboss.server.data.dir}/hornetq/bindings</bindings-directory>" + "\n" +
         "<journal-directory>${jboss.server.data.dir}/hornetq/journal</journal-directory>" + "\n" +
         "<journal-min-files>10</journal-min-files>" + "\n" +
         "<large-messages-directory>${jboss.server.data.dir}/hornetq/largemessages</large-messages-directory>" + "\n" +
         "<paging-directory>${jboss.server.data.dir}/hornetq/paging</paging-directory>" + "\n" +
         "<connectors>" + "\n" +
         "<connector name=\"netty\">" + "\n" +
         "<factory-class>org.hornetq.core.remoting.impl.netty.NettyConnectorFactory</factory-class>" + "\n" +
         "<param key=\"host\"  value=\"${jboss.bind.address:localhost}\"/>" + "\n" +
         "<param key=\"port\"  value=\"${hornetq.remoting.netty.port:5445}\"/>" + "\n" +
         "</connector>" + "\n" +
         "<connector name=\"netty-throughput\">" + "\n" +
         "<factory-class>org.hornetq.core.remoting.impl.netty.NettyConnectorFactory</factory-class>" + "\n" +
         "<param key=\"host\"  value=\"${jboss.bind.address:localhost}\"/>" + "\n" +
         "<param key=\"port\"  value=\"${hornetq.remoting.netty.batch.port:5455}\"/>" + "\n" +
         "<param key=\"batch-delay\" value=\"50\"/>" + "\n" +
         "</connector>" + "\n" +
         "<connector name=\"in-vm\">" + "\n" +
         "<factory-class>org.hornetq.core.remoting.impl.invm.InVMConnectorFactory</factory-class>" + "\n" +
         "<param key=\"server-id\" value=\"${hornetq.server-id:0}\"/>" + "\n" +
         "</connector>" + "\n" +
         "</connectors>" + "\n" +
         "<acceptors>" + "\n" +
         "<acceptor name=\"netty\">" + "\n" +
         "<factory-class>org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory</factory-class>" + "\n" +
         "<param key=\"host\"  value=\"${jboss.bind.address:localhost}\"/>" + "\n" +
         "<param key=\"port\"  value=\"${hornetq.remoting.netty.port:5445}\"/>" + "\n" +
         "</acceptor>" + "\n" +
         "<acceptor name=\"netty-throughput\">" + "\n" +
         "<factory-class>org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory</factory-class>" + "\n" +
         "<param key=\"host\"  value=\"${jboss.bind.address:localhost}\"/>" + "\n" +
         "<param key=\"port\"  value=\"${hornetq.remoting.netty.batch.port:5455}\"/>" + "\n" +
         "<param key=\"batch-delay\" value=\"50\"/>" + "\n" +
         "<param key=\"direct-deliver\" value=\"false\"/>" + "\n" +
         "</acceptor>" + "\n" +
         "<acceptor name=\"in-vm\">" + "\n" +
         "<factory-class>org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory</factory-class>" + "\n" +
         "<param key=\"server-id\" value=\"0\"/>" + "\n" +
         "</acceptor>" + "\n" +
         "</acceptors>" + "\n" +
         "<security-settings>" + "\n" +
         "<security-setting match=\"#\">" + "\n" +
         "<permission type=\"createNonDurableQueue\" roles=\"guest\"/>" + "\n" +
         "<permission type=\"deleteNonDurableQueue\" roles=\"guest\"/>" + "\n" +
         "<permission type=\"create-durable-queue\" roles=\"guest\"/>" + "\n" +
         "<permission type=\"delete-durable-queue\" roles=\"guest\"/>" + "\n" +
         "<permission type=\"consume\" roles=\"guest\"/>" + "\n" +
         "<permission type=\"send\" roles=\"guest\"/>" + "\n" +
         "</security-setting>" + "\n" +
         "</security-settings>" + "\n" +
         "<address-settings>" + "\n" +
         "<address-setting match=\"#\">"
         + "\n" + "<dead-letter-address>jms.queue.DLQ\n</dead-letter-address>" + "\n"
         + "<expiry-address>jms.queue.ExpiryQueue\n</expiry-address>" + "\n"
         + "<redelivery-delay>0\n</redelivery-delay>" + "\n"
         + "<max-size-bytes>10485760\n</max-size-bytes>" + "\n"
         + "<message-counter-history-day-limit>10</message-counter-history-day-limit>" + "\n"
         + "<address-full-policy>BLOCK</address-full-policy>" + "\n" +
         "</address-setting>" + "\n" +
         "</address-settings>" + "\n" +
         "</configuration>";
}
