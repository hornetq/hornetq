/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.jboss.jms.MessageImpl;
import org.jboss.jms.TextMessageImpl;
import org.jboss.jms.destination.JBossDestination;
import org.jboss.logging.Logger;
import org.jboss.system.ServiceMBeanSupport;

/**
 * @jmx:mbean extends="org.jboss.system.ServiceMBean
 *
 * @author <a href="mailto:nathan@jboss.org">Nathan Phelps</a>
 * @version $Revision$ $Date$
 */
public class Server extends ServiceMBeanSupport implements ServerMBean
{
    private Logger log = Logger.getLogger(this.getClass());
    private Map destinations = new HashMap();
    private List deliveryEndpointWorkQueue = new ArrayList();
    private boolean run = true;

    /**
     * @jmx:managed-attribute
     */
    public synchronized void addDestination(
            JBossDestination name,
            Destination destination)
    {
        destination.setServer(this);
        this.destinations.put(name, destination);
    }

    /**
     * @jmx:managed-attribute
     */
    public synchronized void removeDestination(JBossDestination name)
    {
        this.destinations.remove(name);
    }

    void scheduleDelivery(DeliveryEndpoint deliveryEndpoint)
    {
        synchronized (this.deliveryEndpointWorkQueue)
        {
            if (!this.deliveryEndpointWorkQueue.contains(deliveryEndpoint))
            {
                this.deliveryEndpointWorkQueue.add(deliveryEndpoint);
                this.deliveryEndpointWorkQueue.notify();
            }
        }
    }

    private DeliveryEndpoint getNextDeliveryEndpoint()
    {
        while (true)
        {
            DeliveryEndpoint deliveryEndpoint;
            synchronized (this.deliveryEndpointWorkQueue)
            {
                if (this.deliveryEndpointWorkQueue.isEmpty())
                {
                    try
                    {
                        this.deliveryEndpointWorkQueue.wait();
                        continue;
                    }
                    catch (InterruptedException e)
                    {
                    }
                }
                deliveryEndpoint =
                        (DeliveryEndpoint) this.deliveryEndpointWorkQueue.remove(0);
                if (!this.deliveryEndpointWorkQueue.isEmpty())
                {
                    this.deliveryEndpointWorkQueue.notifyAll();
                }
                return deliveryEndpoint;
            }
        }
    }

    private class DeliveryWorker implements Runnable
    {
        private Server server = null;

        DeliveryWorker(Server server)
        {
            this.server = server;
        }

        public void run()
        {
            DeliveryEndpoint deliveryEndpoint;
            while (run)
            {
                deliveryEndpoint = server.getNextDeliveryEndpoint();
                deliveryEndpoint.deliver();
            }
        }
    }

    //////////////////////////////// TEST CODE ////////////////////////////////
    public void startWorkers(int workers)
    {
        for (int i = 0; i < workers; i++)
        {
            Thread worker = new Thread(new Server.DeliveryWorker(this));
            worker.setDaemon(true);
            worker.start();
        }
    }

    private void stopWorkers()
    {
        this.run = false;
    }

    public List addConsumers(int number)
    {
        Destination destination =
                (Destination) this.destinations.get(
                        new JBossDestination("testDestination"));
        List consumers = new ArrayList();
        for (int i = 0; i < number; i++)
        {
            Consumer consumer = new NonDurableConsumer();
            destination.addConsumer(consumer);
            consumers.add(consumer);
        }
        return consumers;
    }

    public void setupTestDestination()
    {
        // To change to a Queue behavior, modify "new TopicDeliveryHandler()"
        // to "new QueueDeliveryHandler()"
        Destination destination =
                new Destination(
                        new SimpleMessageStore(),
                        new TopicDeliveryHandler(),
                        new SimpleConsumerGroup());
        this.addDestination(
                new JBossDestination("testDestination"),
                destination);
    }

    public void sendMessages(int number)
    {
        Destination destination =
                (Destination) this.destinations.get(
                        new JBossDestination("testDestination"));
        for (int i = 0; i < number; i++)
        {
            try
            {
                TextMessageImpl message =
                        (TextMessageImpl) MessageImpl.create(TextMessage.class);
                message.setText("This is message #" + i);
                destination.deliver(message);
            }
            catch (JMSException e)
            {
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]) throws Exception
    {
        System.setProperty(
                "org.jboss.jms.server.consumer.metadata.statistics.enabled",
                "true");
        Server server = new Server();
        server.startWorkers(5);
        server.setupTestDestination();
        List consumers = server.addConsumers(5);
        server.sendMessages(1000);
        System.out.println(
                "Finished sending messages.  Sleeping to wait for consumers to finish.");
        Thread.sleep(5000);
        Iterator iterator = consumers.iterator();
        while (iterator.hasNext())
        {
            System.out.println(
                    ((Consumer) iterator.next()).getMetaData().toString());
        }
        server.stopWorkers();
    }

}