package org.hornetq.stomp.tests;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HornetQTestServerManager
{
   
   List<HornetQTestServer> serverList = new ArrayList<HornetQTestServer>();
   Map<Integer, HornetQTestServer> servers = new LinkedHashMap<Integer, HornetQTestServer>();

   public void createServer(int serverId)
   {
      HornetQTestServer server = new HornetQTestServer(serverId);
      server.start();
      servers.put(serverId, server);
   }

   public void shutdownServer(int serverIndex)
   {
      HornetQTestServer server = servers.remove(serverIndex);
      server.shutdown();
   }

}
