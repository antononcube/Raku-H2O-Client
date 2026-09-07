use v6.d;

class X::H2O is Exception {
    has Str $.message = 'H2O client error';
}

class X::H2O::HTTP is X::H2O {
    has Str $.method is required;
    has Str $.url is required;
    has Int $.status is required;
    has Str $.body = '';

    method message() {
        "H2O request {$!method} {$!url} failed with HTTP {$!status}" ~
        ($!body.chars ?? ": {$!body}" !! '')
    }
}

class X::H2O::Job is X::H2O { }
