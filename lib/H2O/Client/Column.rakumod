use v6.d;

class H2O::Client::Column {
    has $.frame is required;
    has Str:D $.name is required;

    method !metadata() { $!frame.column-metadata($!name) }
    method type() { self!metadata<type> }
    method domain() { (self!metadata<domain> // []).Array }
    method summary() { $!frame.summary(column => $!name) }
    method missing-count() { self!metadata<missing_count> }
    method zero-count() { self!metadata<zero_count> }
    method min() { self!metadata<mins> }
    method max() { self!metadata<maxs> }
    method mean() { self!metadata<mean> }
    method sigma() { self!metadata<sigma> }
    method percentiles() { self!metadata<percentiles> }
    method expression() { $!frame.expression.col($!name) }
    method as-factor() { self.expression.as-factor }
    method gist() { "H2O::Column({$!frame.id}���{$!name})" }
}
