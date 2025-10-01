#!/usr/bin/env raku
use v6.d;

#use lib <. lib>;
use H2O::Client::Connector;

my $h2o = H2O::Client::Connector.new;

%*ENV<JAVA_HOME> = $*HOME ~ '/Library/Java/JavaVirtualMachines/corretto-17.0.10/Contents/Home';

# 1) Start JAR (or connect if already up on that port)
$h2o.init(jar-path => ($*HOME ~ '/Downloads/h2o-3.46.0.7/h2o.jar'), port => 54321, jvm-opts => <-Xmx4g>);

# 2) Check status
say '$h2o.is-running : ', $h2o.is-running;  # { :proc(True), :api(True), :ok(True) }

# 3) Connect to an already-running instance
$h2o.connect(:host('127.0.0.1'), :port(54321));

# 4) Graceful shutdown (REST, with process fallback if we started it)
#$h2o.shutdown;
