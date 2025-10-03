use v6.d;

use HTTP::Tiny;
use JSON::Fast;
use Hash::Merge;
use H2O::Client::Connector;
use Data::Translators;

class H2O::Client {
    has Str:D $.base-url is required;
    has Int:D $.timezone is rw = $*TZ;
    has H2O::Client::Connector $!conn handles <connect is-running shutdown>;

    submethod BUILD(Str:D :$!base-url = 'http://127.0.0.1:54321', Int:D :$!timezone = $*TZ) {}

    multi method new($base-url = Whatever, $timezone = Whatever) {
        self.new(:$base-url, :$timezone)
    }
    multi method new(:$base-url is copy = Whatever, :tz(:$timezone) is copy = Whatever) {
        if $base-url.isa(Whatever) { $base-url = 'http://127.0.0.1:54321' }
        if $timezone.isa(Whatever) { $timezone = $*TZ }
        self.bless(:$timezone)
    }

    #======================================================
    # Core access methods
    #======================================================
    #| Core GET access to H2O
    proto method get(|) {*}

    multi method get(Str:D $path is copy, %query = %(), Bool:D :$echo = False) {
        self.get(:$path, :%query, :$echo)
    }

    multi method get(Str:D :$path is copy, :%query = %(), Bool:D :$echo = False) {
        my $query = %query.map({ "{$_.key}={$_.value}" }).join('&');
        $path .= subst(/^ '/' /);
        my $url = $query ?? "{$!base-url}/{$path}?{$query}" !! "{$!base-url}/{$path}";

        note (:$url) if $echo;

        my $res = HTTP::Tiny.get($url);
        $res = $res<content>.decode;
        try {
            my %res = from-json($res);
            note "Non-OK status {%res<http_status>}" if %res<http_status> && %res<http_status> != 200;
            return %res;
        }
        note 'Cannot process H2O result.';
        return $res;
    }

    #| Core POST access to H2O
    proto method post(|) {*}

    multi method post(Str:D $path is copy, %content, Bool:D :$echo = False) {
        self.post(:$path, :%content, :$echo)
    }

    multi method post(Str:D :$path is copy, :data(:%content), Bool:D :$echo = False) {

        $path .= subst(/^ '/' /);
        my $url = "{$!base-url}/{$path}";

        note (:$url) if $echo;

        my $res = HTTP::Tiny.post($url, headers => %(Content-Type => 'application/x-www-form-urlencoded'), :%content);
        $res = $res<content>.decode;
        try {
            my %res = from-json($res);
            note "Non-OK status {%res<http_status>}" if %res<http_status> && %res<http_status> != 200;
            return %res;
        }
        note 'Cannot process H2O result.';
        return $res;
    }

    #======================================================
    # Initialization
    #======================================================
    method init(*%args) {
        my $res = $!conn.init(|%args);
        $!base-url = $!conn.base-url();
        return $res;
    }

    #======================================================
    # Queries
    #======================================================

    #| Jobs at the H2O cluster
    proto method jobs(|) {*}

    multi method jobs($format) {
        return self.jobs(:$format)
    }

    multi method jobs(:f(:$format) is copy = Whatever) {
        if $format.isa(Whatever) { $format = 'summary' }
        die 'The argument format is expected to be Whatever or one of "asis", "dataset", "html" "summary".'
        unless $format ~~ Str:D && $format.lc ∈ <asis dataset html jobs summary>;

        my $res = self.get('3/Jobs');
        die 'Unexpected result by 3/Jobs.' unless $res ~~ Map:D;

        return do given $format.lc {
            when $_ eq 'asis' { $res }
            when $_ ∈ <dataset jobs> { $res<jobs> }
            when $_ eq 'summary'  {
                $res<jobs>.map({
                    my %h =
                            merge-hash(
                            merge-hash($_<key>.grep(*.key ne '__meta').Hash, %(dest_name => $_<dest><name>, dest_type => $_<dest><type>)),
                                    $_.grep(*.key ∈ <status progress description start_time msec>).Hash);
                    %h<start_time> = DateTime.new(floor(%h<start_time> / 1000), :$!timezone).hh-mm-ss;
                    %h
                }).Array
            }
            when $_ ∈ <html html-table> {
                my $ds = self.jobs(format => 'summary');
                to-html($ds, field-names => <name description status progress start_time msec dest_name dest_type type URL>, align => 'left')
            }
        }
    }

    #| Frames at the H2O cluster
    proto method frames(|) {*}

    multi method frames($format) {
        return self.frames(:$format)
    }

    multi method frames(:f(:$format) is copy = Whatever) {
        if $format.isa(Whatever) { $format = 'summary' }
        die 'The argument format is expected to be Whatever or one of "asis", "dataset", "html", "summary".'
        unless $format ~~ Str:D && $format.lc ∈ <asis dataset frames html summary>;

        my $res = self.get('3/Frames');
        die 'Unexpected result by 3/Frames.' unless $res ~~ Map:D;

        return do given $format.lc {
            when $_ eq 'asis' { $res }
            when $_ ∈ <dataset frames> { $res<frames> }
            when $_ eq 'summary' {
                $res<frames>.map({ merge-hash($_<frame_id>, $_.grep(*.key ∈ <rows columns is_text>).Hash) }).Array
            }
            when $_ ∈ <html html-table> {
                my $ds = self.frames(format => 'summary');
                to-html($ds, field-names => <name rows columns is_text type URL>, align => 'left')
            }
        }
    }

    #| Models at the H2O cluster
    proto method models(|) {*}

    multi method models($format) {
        return self.models(:$format)
    }

    multi method models(:f(:$format)  is copy = Whatever) {
        if $format.isa(Whatever) { $format = 'summary' }
        die 'The argument format is expected to be Whatever or one of "asis", "dataset", "html", "summary".'
        unless $format ~~ Str:D && $format.lc ∈ <asis dataset html models summary>;

        my $res = self.get('3/Models');
        die 'Unexpected result by 3/Models.' unless $res ~~ Map:D;

        return do given $format.lc {
            when $_ eq 'asis' { $res }
            when $_ ∈ <dataset models> { $res<models> }
            when $_ eq 'summary' {
                $res<models>.map({
                    merge-hash(
                            merge-hash($_<model_id>.grep(*.key ne '__meta').Hash, %(data_frame => $_<data_frame><name>)),
                            $_.grep(*.key ∈ <algo algo_full_name response_column_name have_mojo have_pojo>).Hash)
                }).Array
            }
            when $_ ∈ <html html-table> {
                my $ds = self.models(format => 'summary');
                to-html($ds, field-names => <name algo algo_full_name data_frame response_column_name have_mojo have_pojo type URL>, align => 'left')
            }
        }
    }

    #======================================================
    # Frames fundamental operations
    #======================================================
    method data-import($path) {
        my $res = self.get('3/ImportFiles', %(:$path));
        die 'Unexpected result by 3/ImportFiles.' unless $res ~~ Map:D;
        return $res;
    }

    method data-parse-setup(@source-frames) {
        my %content = source_frames => @source-frames;
        my $res = self.post(path => '3/ParseSetup', :%content);
        die 'Unexpected result by 3/ParseSetup.' unless $res ~~ Map:D;
        return $res;
    }

    method data-parse(%props) {
        my $res = self.post(path => '3/Parse', content => %props);
        die 'Unexpected result by 3/Parse.' unless $res ~~ Map:D;
        return $res;
    }

    #======================================================
    # Models fundamental operations
    #======================================================
    method model-build($algo, %props) {

        my $res = self.post("3/ModelBuilders/$algo", %props);

        die "Unexpected result by 3/ModelBuilders/$algo." unless $res ~~ Map:D;

        return $res;
    }

    method model-predict(
            Str:D $model-id,          #= Model ID
            Str:D $frame-name,        #= Frame with data to predictions for
            Str:D $predictions-frame, #= Frame to put the predictions in
            ) {
        my %content =
                predictions_frame => $predictions-frame;

        my $path = "3/Predictions/models/$model-id/frames/$frame-name";
        my $res = self.post(:$path, :%content);

        die "Unexpected result by $path." unless $res ~~ Map:D;

        return $res;
    }
}