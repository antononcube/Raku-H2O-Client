use v6.d;

use JSON::Fast;

# A GroupBy is a lazy Rapids plan, not a server-side resource.  It becomes an
# expression only after one or more aggregations have been selected.
class H2O::Client::Rapids::GroupBy {
    has $.source is required;
    has @.by is required;
    has @!aggregations;

    submethod BUILD(:$!source, :@by) {
        @!by = @by;
        die 'Grouping requires at least one column.' unless @!by.elems;
        die 'Group-by columns must be names or non-negative indexes.'
            unless @!by.all ~~ (Str:D | UInt:D);
    }

    method !add(Str:D $operation, $columns, Str:D $na) {
        die "Unknown NA handling '$na'. Expected all, ignore, or rm."
            unless $na eq any <all ignore rm>;

        my @columns = $columns ~~ Positional ?? $columns.list !! ($columns,);
        @columns = (Nil,) if $columns ~~ Whatever;
        for @columns -> $column {
            die 'Aggregation columns must be names or non-negative indexes.'
                unless !$column.defined || $column ~~ (Str:D | UInt:D);
            @!aggregations.push: [$operation, $column, $na];
        }
        self
    }

    method count(Str:D :$na = 'all') { self!add('nrow', 0, $na) }
    method mean($columns = Whatever, Str:D :$na = 'all') { self!add('mean', $columns, $na) }
    method sum($columns = Whatever, Str:D :$na = 'all') { self!add('sum', $columns, $na) }
    method min($columns = Whatever, Str:D :$na = 'all') { self!add('min', $columns, $na) }
    method max($columns = Whatever, Str:D :$na = 'all') { self!add('max', $columns, $na) }

    method !frame() {
        die 'Named group-by columns require an expression originating from a frame.'
            unless $!source.source-id.defined;
        $!source.client.frame($!source.source-id)
    }

    method !index($column, $frame) {
        return $column if $column ~~ UInt:D;
        my $index = $frame.names.first($column, :k);
        die "Column '$column' does not exist in frame '{ $frame.id }'." unless $index.defined;
        $index
    }

    method expression() {
        die 'A group-by expression requires at least one aggregation.' unless @!aggregations.elems;

        my $needs-frame = @!by.grep(* ~~ Str:D).elems ||
            @!aggregations.grep({ !.[1].defined || .[1] ~~ Str:D }).elems;
        my $frame = self!frame if $needs-frame;
        my @by = @!by.map({ self!index($_, $frame) });
        my @arguments;
        for @!aggregations -> $aggregation {
            my ($operation, $column, $na) = @$aggregation;
            my @columns = $column.defined
                ?? (self!index($column, $frame),)
                !! (^$frame.ncol).grep(* != any @by);
            @arguments.append: $operation, $_, to-json($na) for @columns;
        }
        die 'A group-by expression requires at least one aggregation column.' unless @arguments.elems;

        my $ast = "(GB { $!source.ast } [{ @by.join(' ') }] { @arguments.join(' ') })";
        $!source.WHAT.new(:client($!source.client), :$ast,
            source-id => $!source.source-id)
    }

    method materialize(Str:D $id) { self.expression.materialize($id) }
    method evaluate() { self.expression.evaluate }
    method gist() { "H2O::Rapids::GroupBy<{ $!source.ast }>" }
}
