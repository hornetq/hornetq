package org.hornetq.core.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.tests.util.ServiceTestBase;

public final class ServerLocatorSerializationTest extends TestCase
{

   /**
    * @see "https://issues.jboss.org/browse/HORNETQ-884"
    * @throws Exception
    */
   public void testSerialization() throws Exception
   {
      ServerLocatorImpl locator =
               new ServerLocatorImpl(true, ServiceTestBase.getNettyConnectorTransportConfiguration(true));
      locator.start(Executors.newSingleThreadExecutor());
      assertSerializable(locator);
   }

   void assertSerializable(Serializable object) throws IOException
   {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(out);
      oos.writeObject(object);
      oos.close();
      assertTrue(out.toByteArray().length > 0);
   }
}
