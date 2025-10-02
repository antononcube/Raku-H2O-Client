use v6.d;

use HTTP::Tiny;
use JSON::Fast;
use Hash::Merge;
use H2O::Client::Connector;
use Data::Translators;

class H2O::Client {
    has $.base-url is required;
    has H2O::Client::Connector $!conn handles <connect is-running shutdown>;

    submethod BUILD(:$!base-url = 'http://127.0.0.1:54321') {}

    multi method new($base-url = Whatever) {
        self.new(:$base-url)
    }
    multi method new(:$base-url = Whatever) {
        $base-url.isa(Whatever) ?? self.bless() !! self.bless(:$base-url)
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
        die 'Unexpected 3/Jobs result.' unless $res ~~ Map:D;

        return do given $format.lc {
            when $_ eq 'asis' { $res }
            when $_ ∈ <dataset jobs> { $res<jobs> }
            when $_ eq 'summary'  {
                $res<jobs>.map({
                    my %h =
                            merge-hash(
                            merge-hash($_<key>.grep(*.key ne '__meta').Hash, %(dest_name => $_<dest><name>, dest_type => $_<dest><type>)),
                                    $_.grep(*.key ∈ <status progress description start_time msec>).Hash);
                    %h<start_time> = DateTime.new(%h<start_time>).hh-mm-ss;
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
        die 'Unexpected 3/Frames result.' unless $res ~~ Map:D;

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
        die 'Unexpected 3/Models result.' unless $res ~~ Map:D;

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

}