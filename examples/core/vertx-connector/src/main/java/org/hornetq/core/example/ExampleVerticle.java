package org.hornetq.core.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.platform.Verticle;

public class ExampleVerticle extends Verticle
{
   @Override
   public void start()
   {
      EventBus eventBus = vertx.eventBus();

      final CountDownLatch latch0 = new CountDownLatch(1);

      // Register a handler on the outgoing connector's address
      eventBus.registerHandler(VertxConnectorExample.OUTGOING, 
               new Handler<Message<?>>() {
                  @Override
                  public void handle(Message<?> startMsg)
                  {
                     Object body = startMsg.body();
                     System.out.println("Verticle receives a message: " + body);
                     VertxConnectorExample.result.set(VertxConnectorExample.MSG.equals(body));
                     latch0.countDown();
                     //Tell the example to finish.
                     VertxConnectorExample.latch.countDown();
                  }
      });

      try
      {
         latch0.await(5000, TimeUnit.MILLISECONDS);
      }
      catch (InterruptedException e)
      {
      }
   }
}
