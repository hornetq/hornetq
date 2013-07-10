#!/usr/bin/env ruby

require 'qpid_proton'

messenger = Qpid::Proton::Messenger.new()

begin
  messenger.start
rescue ProtonError => error
  puts "ERROR: #{error.message}"
  puts error.backtrace.join("\n")
  exit
end

  begin
    messenger.subscribe("127.0.0.1:5672/testQueue")
  rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    exit
  end

msg = Qpid::Proton::Message.new

  begin
    messenger.receive(10)
  rescue Qpid::Proton::ProtonError => error
    puts "ERROR: #{error.message}"
    exit
  end

  while messenger.incoming.nonzero?
    begin
      messenger.get(msg)
    rescue Qpid::Proton::Error => error
      puts "ERROR: #{error.message}"
      exit
    end

    puts "Address: #{msg.address}"
    puts "Subject: #{msg.subject}"
    puts "Content: #{msg.content}"
    puts "Message ID: #{msg.id}"
  end

messenger.stop

