/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hornetq.core.server;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.hornetq.utils.ReusableLatch;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NetworkHealthTestCheckTest
{

   Set<NetworkHealthCheck> list = new HashSet<NetworkHealthCheck>();

   NetworkHealthCheck addCheck(NetworkHealthCheck check)
   {
      list.add(check);
      return check;
   }

   HttpServer httpServer;

   final ReusableLatch latch = new ReusableLatch(1);

   HornetQComponent component = new HornetQComponent()
   {
      boolean started = true;

      @Override
      public void start() throws Exception
      {
         started = true;
         latch.countDown();
      }

      @Override
      public void stop() throws Exception
      {
         started = false;
         latch.countDown();
      }

      @Override
      public boolean isStarted()
      {
         return started;
      }
   };

   @Before
   public void before() throws Exception
   {
      latch.setCount(1);
   }


   private void startHTTPServer() throws IOException
   {
      Assert.assertNull(httpServer);
      InetSocketAddress address = new InetSocketAddress("127.0.0.1", 8080);
      httpServer = HttpServer.create(address, 100);
      httpServer.start();
      httpServer.createContext("/", new HttpHandler()
      {
         @Override
         public void handle(HttpExchange t) throws IOException
         {
            String response = "<html><body><b>This is a unit test</b></body></html>";
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
         }
      });
   }


   private void stopHTTPServer()
   {
      if (httpServer != null)
      {
         try
         {
            httpServer.stop(0);
         }
         catch (Throwable ignored)
         {
         }
         httpServer = null;
      }
   }

   @After
   public void after()
   {
      stopHTTPServer();
      for (NetworkHealthCheck check : this.list)
      {
         check.stop();
      }
   }

   @Test
   public void testCheck() throws Exception
   {
      NetworkHealthCheck check = addCheck(new NetworkHealthCheck(null, 100, 100));
      check.addComponent(component);

      // Accordingly to RFC5737, this address is guaranteed to not exist as it is reserved for documentation
      check.addAddress(InetAddress.getByName("203.0.113.1"));

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertFalse(component.isStarted());

      latch.setCount(1);

      // if this latch is solved it means the stop was called again which is wrong
      Assert.assertFalse(latch.await(1, TimeUnit.SECONDS));
      Assert.assertFalse(component.isStarted());

      InetAddress address = InetAddress.getByName("127.0.0.1");

      check.addAddress(address);

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertTrue(component.isStarted());


   }


   @Test
   public void testChecExternalAddress() throws Exception
   {
      NetworkHealthCheck check = addCheck(new NetworkHealthCheck(null, 100, 100));
      check.addComponent(component);

      // Any external IP, to make sure we would use a PING
      InetAddress address = InetAddress.getByName("www.apache.org");

      Assert.assertTrue(check.check(address));

   }

   @Test
   public void testCheckNoNodes() throws Exception
   {
      NetworkHealthCheck check = addCheck(new NetworkHealthCheck());
      Assert.assertTrue(check.check());
   }


   @Test
   public void testCheckUsingHTTP() throws Exception
   {

      startHTTPServer();

      NetworkHealthCheck check = addCheck(new NetworkHealthCheck(null, 100, 1000));

      Assert.assertTrue(check.check(new URL("http://localhost:8080")));

      stopHTTPServer();

      Assert.assertFalse(check.check(new URL("http://localhost:8080")));

      check.addComponent(component);

      URL url = new URL("http://localhost:8080");
      Assert.assertFalse(check.check(url));

      startHTTPServer();

      Assert.assertTrue(check.check(url));

      check.addURL(url);

      Assert.assertFalse(latch.await(500, TimeUnit.MILLISECONDS));
      Assert.assertTrue(component.isStarted());

      // stopping the web server should stop the component
      stopHTTPServer();

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertFalse(component.isStarted());

      latch.setCount(1);

      startHTTPServer();

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertTrue(component.isStarted());


   }

}
