use v6.d;

use JSON::Fast;

class H2O::Client::Rapids::Expr {
    has $.client is required;
    has Str:D $.ast is required;

    method frame(::?CLASS:U: $client, Str:D $id --> H2O::Client::Rapids::Expr) {
        self.new(:$client, ast => self.identifier($id))
    }

    method identifier(::?CLASS:U: Str:D $id --> Str:D) {
        die "Unsafe Rapids identifier '$id'."
        unless $id ~~ /^ <[A..Za..z0..9_.:\-]>+ $/;
        $id
    }

    method !argument($value --> Str:D) {
        given $value {
            when H2O::Client::Rapids::Expr {
                die 'Cannot combine Rapids expressions from different H2O clients.' unless .client === $!client;
                .ast
            }
            when Bool { $_ ?? 'TRUE' !! 'FALSE' }
            when Numeric { .Str }
            when Str { to-json($_) }
            when Positional { '[' ~ .map({ self!argument($_) }).join(' ') ~ ']' }
            when Any:U { '[]' }
            default { die "Unsupported Rapids argument type: '{.^name}'." }
        }
    }

    method !operation(Str:D $operator, *@arguments --> H2O::Client::Rapids::Expr) {
        die "Unsafe Rapids operator '$operator'"
            unless $operator ~~ /^ <[A..Za..z0..9_+*\/%.:<>=!&|?\-]>+ $/;
        my $args = @arguments.map({ self!argument($_) }).join(' ');
        self.WHAT.new(:$!client, ast => "($operator $args)")
    }

    multi method col(Str:D $selector) { self!operation('cols', self, $selector) }
    multi method col(Int:D $selector) { self!operation('cols', self, $selector) }
    method select(*@columns) {
        @columns = @columns.head.List if @columns.elems == 1 && @columns.head ~~ Positional;
        self!operation('cols_py', self, @columns.Array.item)
    }
    method where(H2O::Client::Rapids::Expr:D $predicate) {
        self!operation('rows', self, $predicate)
    }

    method add($value) { self!operation('+', self, $value) }
    method subtract($value) { self!operation('-', self, $value) }
    method multiply($value) { self!operation('*', self, $value) }
    method divide($value) { self!operation('/', self, $value) }
    method modulo($value) { self!operation('%', self, $value) }
    method equal($value) { self!operation('==', self, $value) }
    method not-equal($value) { self!operation('!=', self, $value) }
    method greater-than($value) { self!operation('>', self, $value) }
    method greater-or-equal($value) { self!operation('>=', self, $value) }
    method less-than($value) { self!operation('<', self, $value) }
    method less-or-equal($value) { self!operation('<=', self, $value) }
    method and(H2O::Client::Rapids::Expr:D $other) { self!operation('&', self, $other) }
    method or(H2O::Client::Rapids::Expr:D $other) { self!operation('|', self, $other) }
    method not() { self!operation('!', self) }

    method cbind(*@others) { self!operation('cbind', self, |@others) }
    method rbind(*@others) { self!operation('rbind', self, |@others) }
    method as-factor() { self!operation('as.factor', self) }
    method sum(Bool:D :$remove-na = False) { self!operation('sum', self, $remove-na) }
    method mean(Bool:D :$remove-na = False) { self!operation('mean', self, 0, $remove-na) }

    method materialize(Str:D $id) { $!client.rapids(self, destination => $id) }
    method evaluate() { $!client.rapids(self) }
    method gist() { "H2O::Rapids::Expr<$!ast>" }
}
