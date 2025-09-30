use v6.d;

use HTTP::Tiny;
use JSON::Fast;
use Hash::Merge;

class H2O::Client {
    has $.base-url is required;

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
    # Specific methods
    #======================================================
}