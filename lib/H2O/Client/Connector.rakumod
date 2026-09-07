use v6.d;

use H2O::Client::Transport;

class H2O::Client::Connector {
    has Str:D $.jar-path is rw = 'h2o.jar';
    has Str:D $.host is rw = '127.0.0.1';
    has UInt:D $.port is rw = 54321;
    has Str:D $.java is rw = %*ENV<H2O_JAVA> //
        (%*ENV<JAVA_HOME> ?? %*ENV<JAVA_HOME>.IO.add('bin/java').Str !! 'java');
    has Int:D $.timeout is rw = 10;
    has H2O::Client::Transport $.transport is rw;
    has Proc::Async $!proc;
    has Promise $!completion;
    has Bool:D $.started-by-self is rw = False;

    submethod TWEAK() { self!rebuild-transport unless $!transport.defined }

    method base-url(--> Str:D) { "http://{$!host}:{$!port}" }

    method !rebuild-transport() {
        $!transport = H2O::Client::Transport.new(:base-url(self.base-url), :$!timeout)
    }

    method !ping(--> Bool:D) { so try { $!transport.get('/3/Cloud'); True } }

    method !wait-ready(Numeric:D :$timeout = 60, Numeric:D :$interval = 0.25) {
        my $deadline = now + $timeout;
        repeat {
            return True if self!ping;
            if $!completion && $!completion.status !== Planned {
                my $code = try $!completion.result.exitcode;
                die "H2O process exited with code {$code // 'unknown'} before becoming ready";
            }
            sleep $interval;
        } while now < $deadline;
        die "H2O did not become ready at {self.base-url} within {$timeout}s";
    }

    method is-running(Bool:D :$check-api = True) {
        my $proc-alive = so $!proc && $!proc.started &&
            $!completion && $!completion.status === Planned;
        my $api-ok = $check-api ?? self!ping !! False;
        { :proc($proc-alive), :api($api-ok), :ok($proc-alive || $api-ok) }
    }

    method connect(:$host, :$port, :$timeout) {
        $!host = $host if $host.defined;
        $!port = $port if $port.defined;
        $!timeout = $timeout if $timeout.defined;
        self!rebuild-transport;
        die "Cannot reach H2O at {self.base-url}" unless self!ping;
        $!started-by-self = False;
        $!transport.get('/3/Cloud')
    }

    method init(Str:D :$jar-path = 'h2o.jar', UInt:D :$port = 54321,
                Str:D :$host = '127.0.0.1', :$jvm-opts = [], :$h2o-args = [],
                Numeric:D :$wait-seconds = 60) {
        my @jvm = self!options($jvm-opts);
        my @h2o = self!options($h2o-args);
        $!jar-path = $jar-path;
        $!port = $port;
        $!host = $host;
        self!rebuild-transport;
        if self!ping {
            $!started-by-self = False;
            return $!transport.get('/3/Cloud');
        }
        die "H2O JAR does not exist: $!jar-path" unless $!jar-path.IO.f;
        my @cmd = $!java, |@jvm, '-jar', $!jar-path, |@h2o,
            '-ip', $!host, '-web_ip', $!host, '-port', $!port.Str;
        $!proc = Proc::Async.new(|@cmd, :w, :r, :err);
        $!proc.stdout.tap(-> $line { note $line if %*ENV<H2O_VERBOSE> });
        $!proc.stderr.tap(-> $line { note $line if %*ENV<H2O_VERBOSE> });
        $!completion = $!proc.start;
        $!started-by-self = True;
        self!wait-ready(timeout => $wait-seconds);
        $!transport.get('/3/Cloud')
    }

    method !options($value --> Array) {
        given $value {
            when Positional { .Array }
            when Str { .words.Array }
            default { [] }
        }
    }

    method shutdown(Numeric:D :$wait-seconds = 20, Bool:D :$kill-fallback = True,
                    Bool:D :$cluster = False) {
        return False unless $!started-by-self || $cluster;
        try $!transport.post('/3/Shutdown');
        my $deadline = now + $wait-seconds;
        while now < $deadline && self!ping { sleep 0.25 }
        if $!started-by-self && $!proc && $!completion.status === Planned && $kill-fallback {
            try $!proc.kill(15);
            sleep 0.5;
            try $!proc.kill(9) if $!completion.status === Planned;
        }
        $!started-by-self = False;
        True
    }
}
