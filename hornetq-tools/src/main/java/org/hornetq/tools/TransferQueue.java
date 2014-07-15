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

package org.hornetq.tools;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;

/**
 * @author Clebert Suconic
 */

public class TransferQueue // NO_UCD (unused code)
{

   public void process(String[] arg)
   {
      if (arg.length != 9 && arg.length != 10)
      {
         System.out.println("Invalid number of arguments! " + arg.length);
         printUsage();
         System.exit(-1);
      }

      String sourceHost;
      int sourcePort;
      String queue;

      String targetHost;
      int targetPort;
      String producingAddress;

      int commit;

      int waitTimeout;

      String filter = null;

      try
      {
         sourceHost = arg[1];
         sourcePort = Integer.parseInt(arg[2]);
         queue = arg[3];

         targetHost = arg[4];
         targetPort = Integer.parseInt(arg[5]);
         producingAddress = arg[6];

         waitTimeout = Integer.parseInt(arg[7]);
         commit = Integer.parseInt(arg[8]);

         if (arg.length == 10)
         {
            filter = arg[9];
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         printUsage();
         System.exit(-1);
         return; // the compiler doesn't understand exit as leaving the VM
      }

      Map<String, Object> sourceParameters = new HashMap<String, Object>();
      sourceParameters.put(TransportConstants.HOST_PROP_NAME, sourceHost);
      sourceParameters.put(TransportConstants.PORT_PROP_NAME, sourcePort);

      Map<String, Object> targetParameters = new HashMap<String, Object>();
      sourceParameters.put(TransportConstants.HOST_PROP_NAME, targetHost);
      sourceParameters.put(TransportConstants.PORT_PROP_NAME, targetPort);


      try
      {
         TransportConfiguration configurationSource = new TransportConfiguration(NettyConnectorFactory.class.getName(), sourceParameters);

         ServerLocator locatorSource = HornetQClient.createServerLocator(false, configurationSource);

         ClientSessionFactory factorySource = locatorSource.createSessionFactory();

         ClientSession sessionSource = factorySource.createSession(false, false);

         ClientConsumer consumer;

         if (filter == null)
         {
            consumer = sessionSource.createConsumer(queue);
         }
         else
         {
            consumer = sessionSource.createConsumer(queue, filter);
         }


         TransportConfiguration configurationTarget = new TransportConfiguration(NettyConnectorFactory.class.getName(), targetParameters);

         ServerLocator locatorTarget = HornetQClient.createServerLocatorWithoutHA(configurationSource);



         ClientSessionFactory factoryTarget = locatorSource.createSessionFactory();

         ClientSession sessionTarget = factorySource.createSession(false, false);

         ClientProducer producer = sessionTarget.createProducer(producingAddress);

         sessionSource.start();

         int countMessage = 0;

         while (true)
         {
            ClientMessage message = consumer.receive(waitTimeout);
            if (message == null)
            {
               break;
            }

            message.acknowledge();

            if (!message.containsProperty("_HQ_TOOL_original_address"))
            {
               message.putStringProperty("_HQ_TOOL_original_address", message.getAddress().toString());
            }

            LinkedList<String> listToRemove = new LinkedList<String>();

            for (SimpleString name : message.getPropertyNames())
            {
               if (name.toString().startsWith("_HQ_ROUTE_TO"))
               {
                  listToRemove.add(name.toString());
               }
            }

            for (String str: listToRemove)
            {
               message.removeProperty(str);
            }

            producer.send(message);

            if (++countMessage % commit == 0)
            {
               sessionTarget.commit();
               System.out.println("Sent " + countMessage + " messages");
               sessionSource.commit();
            }
         }

         sessionTarget.commit();
         System.out.println("Totally sent " + countMessage + " messages");
         sessionSource.commit();
         consumer.close();
         producer.close();

         sessionSource.close();
         sessionTarget.close();

         locatorSource.close();
         locatorTarget.close();




      }
      catch (Exception e)
      {
         e.printStackTrace();
         printUsage();
         System.exit(-1);
      }

   }


   public void printUsage()
   {
      for (int i = 0; i < 10; i++)
      {
         System.err.println();
      }
      System.err.println("This method will transfer messages from one queue into another, while removing internal properties such as ROUTE_TO.");
      System.err.println();
      System.err.println(Main.USAGE + " <source-IP> <source-port> <source-queue> <target-IP> <target-port> <target-address> <wait-timeout> <commit-size> [filter]");
      System.err.println();
      System.err.println("source-IP: IP for the originating server for the messages");
      System.err.println("source-port: port for the originating server for the messages");
      System.err.println("source-queue: originating queue for the messages");
      System.err.println();
      System.err.println("target-IP: IP for the destination server for the messages");
      System.err.println("target-port: port for the destination server for the messages");
      System.err.println("target-address: address at the destination server");
      System.err.println();
      System.err.println("wait-timeout: time in milliseconds");
      System.err.println("commit-size: batch size for each transaction (in number of messages)");
      System.err.println();
      System.err.println("filter: You can optionally add a filter to the original queue");
      for (int i = 0; i < 10; i++)
      {
         System.err.println();
      }
   }
}
