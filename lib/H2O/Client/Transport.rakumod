use v6.d;

use HTTP::Tiny;
use JSON::Fast;
use URI::Encode;
use H2O::Client::Exception;

class H2O::Client::Transport {
    has Str:D $.base-url is required;
    has Int:D $.timeout = 10;
    has HTTP::Tiny $!http;

    submethod BUILD(:$!base-url, :$!timeout = 10, :$http) {
        $!base-url .= subst(/\/$/, '');
        $!http = $http // HTTP::Tiny.new(:$!timeout);
    }

    method encode(Str:D $value --> Str:D) { uri_encode_component($value) }

    method path(*@parts --> Str:D) {
        '/' ~ @parts.map({ self.encode(.Str) }).join('/')
    }

    method request(Str:D $method, Str:D $path is copy, :%query = %(),
                   :$content, :$content-type, :$content-length,
                   Bool:D :$raw = False) {
        $path = "/$path" unless $path.starts-with('/');
        my $qs = %query.keys.sort.map({
            my $key = self.encode($_.Str);
            my $value = %query{$_};
            $value ~~ Positional
                ?? $value.map({ "$key={self.encode(.Str)}" }).join('&')
                !! "$key={self.encode($value.Str)}"
        }).join('&');
        my $url = $!base-url ~ $path ~ ($qs.chars ?? "?$qs" !! '');
        my %headers = Accept => ($raw ?? '*/*' !! 'application/json');
        %headers<Content-Type> = $content-type if $content-type.defined;
        %headers<Content-Length> = $content-length if $content-length.defined;
        my %options = :%headers;
        %options<content> = $content if $content.defined;
        my %response = $!http.request($method.uc, $url, |%options);
        my $bytes = %response<content> // Buf.new;
        my $body = $bytes ~~ Blob ?? $bytes.decode !! $bytes.Str;
        unless %response<success> {
            die X::H2O::HTTP.new(:method($method.uc), :$url,
                :status((%response<status> // 599).Int), :$body);
        }
        return $bytes if $raw;
        return Nil unless $body.chars;
        try {
            CATCH { default { die X::H2O.new(message => "Invalid JSON from H2O at $url: {.message}") } }
            return from-json($body);
        }
    }

    method get(Str:D $path, :%query = %(), Bool:D :$raw = False) {
        self.request('GET', $path, :%query, :$raw)
    }

    method post(Str:D $path, :%content = %(), :$file, Bool:D :$raw = False) {
        if $file.defined {
            my $path-object = $file.IO;
            my $handle = $path-object.open(:r, :bin);
            LEAVE $handle.close;
            my &chunks = {
                my $chunk = $handle.read(1024 * 1024);
                $chunk.bytes ?? $chunk !! Nil
            };
            return self.request('POST', $path, content => &chunks,
                content-type => 'application/octet-stream',
                content-length => $path-object.s, :$raw);
        }
        my %wire = %content.map({
            .key => (.value ~~ Positional|Associative ?? to-json(.value) !! .value)
        });
        self.request('POST', $path, content => %wire,
            content-type => 'application/x-www-form-urlencoded', :$raw)
    }

    method delete(Str:D $path) { self.request('DELETE', $path) }
}
