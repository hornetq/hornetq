package org.hornetq.integration.discovery.jgroups;

import java.net.URL;
import java.util.Map;

import org.hornetq.core.cluster.BroadcastEndpoint;
import org.hornetq.utils.ConfigurationHelper;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.util.Util;


public class JGroupsBroadcastEndpoint extends ReceiverAdapter implements BroadcastEndpoint
{
   private String jgroupsConfigurationFileName;
   
   private String jgroupsChannelName = null;
   
   private JChannel broadcastChannel;
   
   private boolean started;

   private JChannel discoveryChannel;
   
   private Message broadcastMsg;

   public JGroupsBroadcastEndpoint()
   {
   }
   
   @Override
   public void init(Map<String, Object> params) throws Exception
   {
      jgroupsConfigurationFileName = ConfigurationHelper.getStringProperty(BroadcastGroupConstants.JGROUPS_CONFIGURATION_FILE_NAME,
                                                                           null,
                                                                           params);
      if (jgroupsConfigurationFileName == null || "".equals(jgroupsConfigurationFileName.trim()))
      {
         throw new IllegalArgumentException("No JGroups configuration file specified.");
      }
      
      jgroupsChannelName = ConfigurationHelper.getStringProperty(BroadcastGroupConstants.JGROUPS_CHANNEL_NAME_NAME, 
                                                                           BroadcastGroupConstants.DEFAULT_JGROUPS_CHANNEL_NAME, 
                                                                           params);
   }

   @Override
   public void broadcast(byte[] data) throws Exception
   {
      Message msg = new Message();
      msg.setBuffer(data);
      broadcastChannel.send(msg);
   }

   @Override
   public synchronized void receive(Message msg)
   {
      //we simply update the message, regardless of whether it has been
      //received or not, which is fine for discovery.
      broadcastMsg = msg;
      notify();
   }
   
   @Override
   public synchronized byte[] receiveBroadcast() throws Exception
   {
      byte[] data = null;
      while (started)
      {
         if (broadcastMsg == null)
         {
            try
            {
               wait();
            }
            catch (InterruptedException e)
            {
               if (!started) throw new Exception(this + " Endpoint stopped.");
            }
         }
         else
         {
            data = broadcastMsg.getBuffer();
            broadcastMsg = null;
            break;
         }
      }
      return data;
   }

   @Override
   public void start(boolean broadcasting) throws Exception
   {
      if (started) return;
      
      URL configURL = Thread.currentThread().getContextClassLoader().getResource(this.jgroupsConfigurationFileName);
      if (broadcasting)
      {
         broadcastChannel = new JChannel(configURL);
         broadcastChannel.connect(this.jgroupsChannelName);
      }
      else
      {
         discoveryChannel = new JChannel(configURL);
         discoveryChannel.setReceiver(this);
         discoveryChannel.connect(this.jgroupsChannelName);
      }
      started = true;
   }

   @Override
   public void stop() throws Exception
   {
      if (broadcastChannel != null)
      {
         Util.shutdown(broadcastChannel);
         broadcastChannel = null;
      }
      
      if (discoveryChannel != null)
      {
         Util.shutdown(discoveryChannel);
         discoveryChannel = null;
      }
      
      started = false;
   }

}
