/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.remoting;

import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.integration.transports.netty.NettyAcceptorFactory;
import org.hornetq.integration.transports.netty.TransportConstants;
import org.hornetq.spi.core.protocol.ProtocolType;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * An AardvarkProtocolTest
 *
 * @author Tim Fox
 *
 *
 */
public class AardvarkProtocolTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(AardvarkProtocolTest.class);

   public void testAardvark() throws Exception
   {
      RemotingServiceImpl.hackProtocol = ProtocolType.AARDVARK;
      
      Configuration config = new ConfigurationImpl();
      
      config.setSecurityEnabled(false);
      config.setPersistenceEnabled(false);
      
      Map<String, Object> params = new HashMap<String, Object>();
      
      params.put(TransportConstants.PORT_PROP_NAME, 9876);
      params.put(TransportConstants.HOST_PROP_NAME, "127.0.0.1");
      
      TransportConfiguration tc = new TransportConfiguration(NettyAcceptorFactory.class.getCanonicalName(),
                                                             params);
      
      config.getAcceptorConfigurations().add(tc);
      
      HornetQServer server = HornetQServers.newHornetQServer(config);
      
      server.start();
      
      //Now we should be able to make a connection to this port and talk the Aardvark protocol!
      
      Socket socket = new Socket("127.0.0.1", 9876);
      
      
      
      OutputStream out = new BufferedOutputStream(socket.getOutputStream());
      
      String s = "AARDVARK!\n";
      
      byte[] bytes = s.getBytes("UTF-8");
      
      out.write(bytes);
      
      out.flush();
      
      InputStream in = socket.getInputStream();

      log.info("writing bytes");
            byte b;
      
      while ((b = (byte)in.read()) != '\n')
      {
         log.info("read " + (char)b);
      }
            
      socket.close();
      
      server.stop();
      
      RemotingServiceImpl.hackProtocol = ProtocolType.CORE;
   }
}
