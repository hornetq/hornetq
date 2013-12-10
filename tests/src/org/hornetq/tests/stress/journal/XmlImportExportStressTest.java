package org.hornetq.tests.stress.journal;

import junit.framework.Assert;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.jms.persistence.impl.journal.XmlDataExporter;
import org.hornetq.jms.persistence.impl.journal.XmlDataImporter;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class XmlImportExportStressTest extends ServiceTestBase
{
   public static final int CONSUMER_TIMEOUT = 5000;

   public void testHighVolume() throws Exception
   {
      final String FILE_NAME = "temp";
      final String QUEUE_NAME = "A1";
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, true, true);

      session.createQueue(QUEUE_NAME, QUEUE_NAME);

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(true);
      final int SIZE = 10240;
      final int COUNT = 20000;
      byte bodyTst[] = new byte[SIZE];
      for (int i = 0; i < SIZE; i++)
      {
         bodyTst[i] = (byte) (i + 1);
      }

      msg.getBodyBuffer().writeBytes(bodyTst);
      assertEquals(bodyTst.length, msg.getBodySize());

      for (int i = 0; i < COUNT; i++)
      {
         producer.send(msg);
         System.out.println("Sent " + i);
      }

      session.close();
      locator.close();
      server.stop();

      System.out.println("Writing XML...");
      FileOutputStream xmlOutputStream = new FileOutputStream(FILE_NAME);
      XmlDataExporter xmlDataExporter = new XmlDataExporter(xmlOutputStream, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      xmlOutputStream.close();
      System.out.println("Done writing XML.");

      clearData();
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      System.out.println("Reading XML...");
      FileInputStream xmlInputStream = new FileInputStream(FILE_NAME);
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session, managementSession);
      xmlDataImporter.processXml();
      xmlInputStream.close();
      System.out.println("Done reading XML.");

      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      for (int i = 0; i < COUNT; i++)
      {
         msg = consumer.receive(CONSUMER_TIMEOUT);
         System.out.println("Received " + i);
         Assert.assertNotNull(msg);
         assertEquals(msg.getBodySize(), bodyTst.length);
         byte bodyRead[] = new byte[bodyTst.length];
         msg.getBodyBuffer().readBytes(bodyRead);
         assertEqualsByteArrays(bodyTst, bodyRead);
      }

      session.close();
      locator.close();
      server.stop();
      File temp = new File(FILE_NAME);
      temp.delete();
   }
}