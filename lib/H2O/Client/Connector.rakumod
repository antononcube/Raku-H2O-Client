use v6.d;

use HTTP::Tiny;
use JSON::Fast;

class H2O::Client::Connector {
    has Str:D $.jar-path is rw = $*CWD.Str;
    has Str:D $.host     is rw = '127.0.0.1';
    has Int:D $.port     is rw = 54321;
    has Str:D $.java     is rw = 'java';
    has Int:D $.timeout  is rw = 10; # Timeout in seconds

    has HTTP::Tiny $.http is built = HTTP::Tiny.new( :$!timeout );
    has Proc::Async $!proc;
    has Bool:D $.started-by-self is rw = False;

    # ------------------ helpers ------------------

    method base-url() { "http://{$!host}:{$!port}" }

    method !headers() { { 'Accept' => 'application/json' } }

    method !request(Str:D $method, Str:D $path, :%data = %()) {
        my $url = self.base-url ~ $path;
        my %opt = :headers(self!headers());
        if %data {
            %opt<headers><Content-Type> = 'application/json';
            %opt<content> = to-json %data;
        }
        my $res = $!http.request($method, $url, |%opt);
        return Nil unless $res<success>;
        return from-json($res<body>) if $res<body> && $res<body>.chars;
        return Nil;
    }

    method !ping(-->Bool:D) {
        # Minimal health probe; /3/Cloud is stable for H2O-3
        my $old = $!http;
        my $tmp = HTTP::Tiny.new(:$!timeout);
        my $res = $tmp.request('GET', self.base-url ~ '/3/Cloud', :headers(self!headers));
        return False unless $res<success>;
        try {
            from-json($res<content>.decode);
            return True;
        }
        return False;
    }

    method !wait-ready(:$timeout = 60, :$interval = 0.25) {
        my $deadline = now + $timeout;
        repeat {
            return True if self!ping();
            sleep $interval;
        } while now < $deadline;
        die "H2O did not become ready at {self.base-url} within {$timeout}s";
    }

    # ------------------ API ------------------

    # Check if H2O is running.
    # Returns a hash with process + api flags; ok => True if either looks alive.
    method is-running(:$check-api = True) {
        my $proc-alive = so $!proc && $!proc.started && $!proc.ready;
        my $api-ok = $check-api ?? self!ping() !! False;
        return { :proc($proc-alive), :api($api-ok), :ok($proc-alive || $api-ok) };
    }

    # Connect to an already-running H2O JAR (we do not spawn it).
    # You may pass :host and :port to override defaults.
    method connect(:$host?, :$port?, :$timeout?) {
        $!host = $host if $host.defined;
        $!port = $port if $port.defined;
        $!timeout = $timeout if $timeout.defined;
        $!http = HTTP::Tiny.new(:$!timeout);

        die "Cannot reach H2O at {self.base-url}" unless self!ping();
        # return cluster info JSON
        return self!request('GET', '/3/Cloud');
    }

    # Start an H2O JAR and wait until the REST API is ready.
    # JVM options go in @jvm-opts (e.g., '-Xmx4g'). Additional H2O args in @h2o-args.
    method init(
            Str:D :$jar-path = $*CWD.Str,
            UInt:D :$port = 54321,
            Str:D :$host = '127.0.0.1',
            :$jvm-opts = Whatever,
            :$h2o-args = Whatever,
            Numeric:D :$wait-seconds = 60) {

        # Process JVM and H2O options
        my @jvm-opts = do given $jvm-opts {
            when $_ ~~ List:D | Array:D | Seq:D { $_.Array }
            when $_ ~~ Str:D { $_.split(/\s/, :skip-empty)».trim.Array }
            default { [] }
        }

        my @h2o-args = do given $h2o-args {
            when $_ ~~ List:D | Array:D | Seq:D { $_.Array }
            when $_ ~~ Str:D { $_.split(/\s/, :skip-empty)».trim.Array }
            default { [] }
        }

        # Keep in the object
        $!jar-path = $jar-path;
        $!port     = $port;
        $!host     = $host;

        # If something is already listening and healthy, just connect.
        if self!ping() {
            $!started-by-self = False;
            return self.connect();
        }

        # Build command: java [jvm-opts...] -jar <jar> [h2o-args...] -port <port>
        my @cmd = ($!java, |@jvm-opts, '-jar', $!jar-path, |@h2o-args, '-port', $!port.Str);

        # Spawn detached server; capture stdout/stderr but don't spam the console.
        $!proc = Proc::Async.new(|@cmd, :w, :r, :err);
        $!proc.stdout.tap({ say $_ });   # discard or log if you prefer
        $!proc.stderr.tap({ note $_ });
        $!proc.start;
        $!started-by-self = True;

        # Wait for HTTP to be ready
        self!wait-ready(:timeout($wait-seconds));
        return self!request('GET', '/3/Cloud');
    }

    #| Shut down H2O.
    #| 1) Try REST /3/Shutdown (graceful)
    #| 2) If we started the process and it's still alive, send a TERM (then KILL).
    method shutdown(:$wait-seconds = 20, :$kill-fallback = True) {
        my $rest-ok = False;
        CATCH { default { } }  # ignore REST errors; fall through to kill if needed

        if self!ping() {
            self!request('POST', '/3/Shutdown');   # H2O exits shortly after 200 OK
            $rest-ok = True;
        }

        my $deadline = now + $wait-seconds;
        repeat {
            my %st = self.is-running();
            last unless %st<api> || %st<proc>;
            sleep 0.5;
        } while now < $deadline;

        # If still alive and we own the process, escalate
        if $!started-by-self && $!proc && !$!proc.exitcode.defined {
            if $kill-fallback {
                try { $!proc.kill(15) }       # TERM
                sleep 1;
                try { $!proc.kill(9) if !$!proc.exitcode.defined }  # KILL
            }
        }
        return True;
    }
}
