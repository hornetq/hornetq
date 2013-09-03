package org.hornetq.core.protocol.stomp;

public enum StompCommands
{
   ABORT("ABORT"),
   ACK("ACK"),
   NACK("NACK"),
   BEGIN("BEGIN"),
   COMMIT("COMMIT"),
   CONNECT("CONNECT"),
   CONNECTED("CONNECTED"),
   DISCONNECT("DISCONNECT"),
   ERROR("ERROR"),
   MESSAGE("MESSAGE"),
   RECEIPT("RECEIPT"),
   SEND("SEND"),
   STOMP("STOMP"),
   SUBSCRIBE("SUBSCRIBE"),
   UNSUBSCRIBE("UNSUBSCRIBE");

   private String command;

   private StompCommands(String command)
   {
      this.command = command;
   }

   @Override
   public String toString()
   {
      return command;
   }

}
