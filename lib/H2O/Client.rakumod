use v6.d;

use JSON::Fast;
use Data::Importers;
use Data::Translators;
use H2O::Client::Connector;
use H2O::Client::Transport;
use H2O::Client::Frame;
use H2O::Client::Job;

class H2O::Client {
    has Str:D $.base-url is rw = 'http://127.0.0.1:54321';
    has Int:D $.timezone is rw = $*TZ;
    has $.transport;
    has H2O::Client::Connector $!connector;

    submethod BUILD(:$!base-url = 'http://127.0.0.1:54321',
                    :tz(:$!timezone) = $*TZ, :$transport, :$connector) {
        $!base-url .= subst(/\/$/, '');
        $!transport = $transport // H2O::Client::Transport.new(:$!base-url);
        $!connector = $connector // H2O::Client::Connector.new;
    }

    multi method new(Str:D $base-url, Int:D :tz(:$timezone) = $*TZ, :$transport) {
        self.bless(:$base-url, :$timezone, :$transport)
    }
    multi method new(Str:D $base-url, Int:D $timezone, :$transport) {
        self.bless(:$base-url, :$timezone, :$transport)
    }
    multi method new(Str:D :$base-url = 'http://127.0.0.1:54321',
                     Int:D :tz(:$timezone) = $*TZ, :$transport, :$connector) {
        self.bless(:$base-url, :$timezone, :$transport, :$connector)
    }

    method encode(Str:D $value --> Str:D) { $!transport.encode($value) }

    method get(Str:D $path, :%query = %(), Bool:D :$raw = False,
               Bool:D :$echo = False) {
        note "GET $path" if $echo;
        $!transport.get($path, :%query, :$raw)
    }
    method post(Str:D $path, :%content = %(), :$file, Bool:D :$raw = False,
                Bool:D :$echo = False) {
        note "POST $path" if $echo;
        $!transport.post($path, :%content, :$file, :$raw)
    }
    method delete(Str:D $path) { $!transport.delete($path) }

    method connect(*%options) {
        my $result = $!connector.connect(|%options);
        $!transport = $!connector.transport;
        $!base-url = $!connector.base-url;
        $result
    }

    method init(*%options) {
        my $result = $!connector.init(|%options);
        $!transport = $!connector.transport;
        $!base-url = $!connector.base-url;
        $result
    }

    method is-running(Bool:D :$check-api = True) {
        my %state = $!connector.is-running(:check-api(False));
        my $api = $check-api ?? so try { self.get('/3/Cloud'); True } !! False;
        %(proc => %state<proc>, :$api, ok => (%state<proc> || $api))
    }
    method shutdown(|args) { $!connector.shutdown(|args) }

    multi method jobs(Str:D $format) { self.jobs(:$format) }
    multi method jobs(Str:D :$format = 'summary') {
        my $response = self.get('/3/Jobs');
        return $response if $format.lc eq 'raw' || $format.lc eq 'asis';
        return ($response<jobs> // []).map({ H2O::Client::Job.from-response(self, $_) }).Array
            if $format.lc eq 'objects' || $format.lc eq 'jobs';
        ($response<jobs> // []).map({
            %(id => (.<key><name> // .<key>),
              destination => (.<dest><name> // .<dest>),
              status => .<status>, progress => .<progress>,
              description => .<description>, msec => .<msec>)
        }).Array
    }

    method frame(Str:D $id, Bool:D :$refresh = False --> H2O::Client::Frame) {
        my $frame = H2O::Client::Frame.new(:client(self), :$id);
        $frame.refresh if $refresh;
        $frame
    }

    multi method frames(Str:D $format) {
        given $format.lc {
            when 'raw'|'asis' { self.frames(:raw) }
            when 'summary'|'dataset' { self.frames(:summary) }
            default { self.frames }
        }
    }
    multi method frames(Bool:D :$raw = False, Bool:D :$summary = False) {
        my $response = self.get('/3/Frames');
        return $response if $raw;
        my @frames = ($response<frames> // []).map({
            my $id = .<frame_id><name> // .<frame_id> // .<key><name> // .<name>;
            self.frame($id.Str)
        }).Array;
        return @frames unless $summary;
        @frames.map({ %(id => .id, rows => .nrows, columns => .ncols,
                        names => .names, types => .types) }).Array
    }

    multi method models(Str:D $format) { self.models(:$format) }
    multi method models(Str:D :$format = 'summary') {
        my $response = self.get('/3/Models');
        return $response if $format.lc eq 'raw' || $format.lc eq 'asis';
        return ($response<models> // []).Array if $format.lc eq 'models' || $format.lc eq 'dataset';
        ($response<models> // []).map({
            %(id => (.<model_id><name> // .<model_id>), algo => .<algo>,
              algorithm => .<algo_full_name>, response-column => .<response_column_name>,
              mojo => .<have_mojo>, pojo => .<have_pojo>)
        }).Array
    }

    method import-file(Str:D $server-path) {
        self.get('/3/ImportFiles', query => %(path => $server-path))
    }

    method upload-file(IO::Path:D $path, Str :$destination-frame,
                       :@column-names, :@column-types, *%parse-options
                       --> H2O::Client::Job) {
        die "Upload file does not exist: $path" unless $path.f;
        my $uploaded = self.post('/3/PostFile', :file($path));
        my $source = $uploaded<destination_frame><name> //
                     $uploaded<destination_frame> // $uploaded<key><name>;
        die 'H2O upload response did not contain a destination frame' unless $source.defined;
        my $setup = self.data-parse-setup([$source]);
        my %props =
            destination_frame => ($destination-frame // "raku-{$*PID}-{now.Int}.hex"),
            source_frames => [($setup<source_frames> // []).head<name> // $source],
            parse_type => $setup<parse_type>, separator => $setup<separator>,
            number_columns => $setup<number_columns>,
            single_quotes => ($setup<single_quotes> // False),
            check_header => ($setup<check_header> // 0),
            column_names => (@column-names.elems ?? @column-names !! $setup<column_names>),
            column_types => (@column-types.elems ?? @column-types !! $setup<column_types>),
            delete_on_done => True;
        %props{$_} = %parse-options{$_} for %parse-options.keys;
        self.data-parse(%props)
    }

    method upload(@records, Str :$destination-frame, :@column-names, :@column-types,
                  *%parse-options --> H2O::Client::Job) {
        my $path = $*TMPDIR.IO.add("h2o-upload-{$*PID}-{now.Int}-{1_000_000.rand.Int}.csv");
        LEAVE try $path.unlink;
        die 'Cannot serialize records as CSV' unless data-export($path.Str, @records, 'csv');
        %parse-options<check_header> //= 1;
        self.upload-file($path, :$destination-frame, :@column-names, :@column-types,
                         |%parse-options)
    }

    multi method data-import(@records, *%options) { self.upload(@records, |%options) }
    multi method data-import(IO::Path:D $path, *%options) { self.upload-file($path, |%options) }
    multi method data-import(Str:D $server-path) { self.import-file($server-path) }

    method data-parse-setup(@source-frames) {
        self.post('/3/ParseSetup', content => %(source_frames => @source-frames))
    }

    method data-parse(%props --> H2O::Client::Job) {
        H2O::Client::Job.from-response(self, self.post('/3/Parse', content => %props))
    }

    method model-build(Str:D $algorithm, %properties --> H2O::Client::Job) {
        H2O::Client::Job.from-response(self,
            self.post("/3/ModelBuilders/{self.encode($algorithm)}", content => %properties))
    }

    method model-predict(Str:D $model-id, Str:D $frame-id,
                         Str:D $predictions-frame --> H2O::Client::Frame) {
        self.post("/3/Predictions/models/{self.encode($model-id)}/frames/{self.encode($frame-id)}",
                  content => %(predictions_frame => $predictions-frame));
        self.frame($predictions-frame)
    }
}
