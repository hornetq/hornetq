/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.microcontainer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.kernel.plugins.bootstrap.basic.BasicBootstrap;
import org.jboss.kernel.plugins.deployment.xml.BeanXMLDeployer;
import org.jboss.kernel.spi.config.KernelConfig;
import org.jboss.kernel.spi.deployment.KernelDeployment;

/**
 * This is the method in which the JBM server can be deployed externall outside of jBoss. Alternatively a user can embed
 * by using the same code as in main
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class JBMBootstrapServer extends BasicBootstrap
{
   /**
    * The deployer
    */
   protected BeanXMLDeployer deployer;
   /**
    * The deployments
    */
   protected List deployments = new CopyOnWriteArrayList();
   /**
    * The arguments
    */
   protected String[] args;
   private Properties properties;

   /**
    * Bootstrap the kernel from the command line
    *
    * @param args the command line arguments
    * @throws Exception for any error
    */
   public static void main(String[] args) throws Exception
   {
      JBMBootstrapServer bootstrap = new JBMBootstrapServer(args);
      bootstrap.run();
      test();


   }


   public void run()
   {
      super.run();
      log.info("JBM Server Started");
   }

   private static void test()
           throws NamingException, JMSException
   {
      //do some JMS stuff
      InitialContext ic = new InitialContext();


      ConnectionFactory cf = (ConnectionFactory) ic.lookup("ConnectionFactory");
      Queue q = (Queue) ic.lookup("queue/testQueue");
      Connection c = cf.createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      TextMessage message = s.createTextMessage("test");
      MessageProducer p = s.createProducer(q);
      p.send(message);
      message = s.createTextMessage("test2");
      p.send(message);
      message = s.createTextMessage("test3");
      p.send(message);
      message = s.createTextMessage("test4");
      p.send(message);
      c.start();
      MessageConsumer mc = s.createConsumer(q);
      MessageListener messageListener = new MessageListener()
      {
         public void onMessage(Message message)
         {
            TextMessage textMessage = (TextMessage) message;
            try
            {
               System.out.println("textMessage.getText() = " + textMessage.getText());
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
         }
      };
      mc.setMessageListener(messageListener);
      c.start();
      c.close();
   }

   /**
    * JBoss 1.0.0 final
    * Standalone
    * Create a new bootstrap
    *
    * @param args the arguments
    * @throws Exception for any error
    */
   public JBMBootstrapServer(String[] args) throws Exception
   {
      super();
      this.args = args;
   }

   public JBMBootstrapServer(String[] args, KernelConfig kernelConfig) throws Exception
   {
      super(kernelConfig);
      //System.setProperty("java.naming.factory.initial", "org.jnp.interfaces.LocalOnlyContextFactory");
      //System.setProperty("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
      this.args = args;
   }
   
   public void bootstrap() throws Throwable
   {
      super.bootstrap();
      deployer = new BeanXMLDeployer(getKernel());
      Runtime.getRuntime().addShutdownHook(new Shutdown());

      for (String arg : args)
      {
         deploy(arg);
      }

      deployer.validate();
   }

   
   /**
    * Undeploy a deployment
    *
    * @param deployment the deployment
    */
   public void undeploy(KernelDeployment deployment) throws Throwable
   {
      log.debug("Undeploying " + deployment.getName());
      deployments.remove(deployment);
      try
      {
         deployer.undeploy(deployment);
         log.debug("Undeployed " + deployment.getName());
      }
      catch (Throwable t)
      {
         log.warn("Error during undeployment: " + deployment.getName(), t);
      }
   }

   public KernelDeployment deploy(String arg) throws Throwable
   {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      URL url = cl.getResource(arg);
      if (url == null)
      {
         url = cl.getResource("META-INF/" + arg);
      }
      //try the system classpath
      if(url == null)
      {
         url = getClass().getClassLoader().getResource(arg);
      }
      if (url == null)
      {
         throw new RuntimeException("Unable to find resource:" + arg);
      }
      return deploy(url);
   }
   
   /**
    * Deploys a XML on the container
    * @author clebert.suconic@jboss.com
    */
   public KernelDeployment deploy(String name, String xml) throws Throwable
   {
      ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
      PrintStream printOut = new PrintStream(byteOut);
      printOut.print(xml);
      printOut.flush();
      ByteArrayInputStream is = new ByteArrayInputStream(byteOut.toByteArray());
    
      KernelDeployment deployment = deployer.deploy(name, is);
      
      deployments.add(deployment);
      
      return deployment;
   }

   
   
   /**
    * Deploy a url
    *
    * @param url the deployment url
    * @throws Throwable for any error
    */
   protected KernelDeployment deploy(URL url) throws Throwable
   {
      log.debug("Deploying " + url);
      KernelDeployment deployment = deployer.deploy(url);
      deployments.add(deployment);
      log.debug("Deployed " + url);
      return deployment;
   }

   public void shutDown()
   {
      log.info("Shutting down");
      ListIterator iterator = deployments.listIterator(deployments.size());
      while (iterator.hasPrevious())
      {
         KernelDeployment deployment = (KernelDeployment) iterator.previous();
         try {undeploy(deployment);} catch (Throwable ignored){}
      }
   }

   protected Properties getConfigProperties()
   {
      return properties;
   }

   public void setProperties(Properties props)
   {
      properties = props;
   }
   protected class Shutdown extends Thread
   {
      public void run()
      {
         JBMBootstrapServer.this.shutDown();
      }
   }
}


