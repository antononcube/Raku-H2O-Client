use v6.d;

use H2O::Client::Column;
use H2O::Client::Rapids::Expr;

class H2O::Client::Frame does Associative {
    has $.client is required;
    has Str:D $.id is required;
    has %!metadata;
    has Bool $!loaded = False;
    has Bool $!deleted = False;

    method identity(--> Str:D) { "{$!client.base-url}#{$!id}" }
    method WHICH() { ValueObjAt.new(self.^name ~ '|' ~ self.identity) }
    method WHAT() { ValueObjAt.new(self.^name) }
    method Str() { $!id }
    method deleted(--> Bool:D) { $!deleted }

    method refresh(Bool:D :$light = True --> H2O::Client::Frame) {
        my %query = row_count => 10, column_count => -1, full_column_count => 0;
        my $suffix = $light ?? '/light' !! '';
        my $response = $!client.get("/3/Frames/{ $!client.encode($!id) }$suffix", :%query);
        %!metadata = ($response<frames> // []).head // {};
        $!loaded = True;
        $!deleted = False;
        self
    }

    method exists(--> Bool:D) {
        return False if $!deleted;
        so try { self.refresh; True }
    }

    method !ensure() { self.refresh unless $!loaded; %!metadata }
    method metadata() { self!ensure.Hash }
    method nrow() { self!ensure<rows>.Int }
    method rows-count() { self.nrow }
    method ncol() { (self!ensure<num_columns> // self!ensure<total_column_count>).Int }
    method columns-count() { self.ncol }
    method shape(Bool:D :p(:$pairs)=False) { $pairs ?? %(nrow => self.nrow, ncol => self.ncol) !! (self.nrow, self.ncol) }
    method dimensions(Bool:D :p(:$pairs)=False) { $pairs ?? %(rows => self.nrow, columns => self.ncol) !! (self.nrow, self.ncol) }
    method names() { (self!ensure<columns> // []).grep(*.defined).map(*<label>).Array }
    method columns() { self.names }
    method types() {
       (self!ensure<columns> // []).grep(*.defined).map({ .<label> => .<type> }).Hash
    }
    method checksum() { self!ensure<checksum> }
    method byte-size() { self!ensure<byte_size> }
    method is-text() { so self!ensure<is_text> }

    method column-metadata(Str:D $name) {
        my $column = (self!ensure<columns> // []).grep(*.defined).first(*<label> eq $name);
        die "Column '$name' does not exist in frame '$!id'." unless $column.defined;
        $column
    }

    method column(Str:D $name --> H2O::Client::Column) {
        self.column-metadata($name);
        H2O::Client::Column.new(:frame(self), :$name)
    }

    method AT-KEY($name) { self.column($name.Str) }
    method EXISTS-KEY($name) { so self.names.first(* eq $name.Str) }

    method preview(UInt:D :$rows = 10, UInt:D :$offset = 0, :$columns = Whatever --> Array) {
        my @all = self.names;
        my @wanted = $columns ~~ (Array:D | List:D | Seq:D) && $columns.elems ?? |$columns !! @all;
        my @indices = @wanted.map({
            my $index = @all.first($_, :k);
            die "Column '$_' does not exist in frame '$!id'." unless $index.defined;
            $index
        });
        # H2O's endpoint pages contiguous columns; request the span and filter locally.
        my $first = @indices.min // 0;
        my $last = @indices.max // -1;
        my %query = row_offset => $offset, row_count => $rows,
            column_offset => $first, column_count => ($last - $first + 1),
            full_column_count => ($last - $first + 1);
        my $response = $!client.get("/3/Frames/{ $!client.encode($!id) }", :%query);
        my @returned = (($response<frames> // []).head<columns> // []).grep(*.defined);
        my %by-name = @returned.map({ .<label> => $_ }).Hash;
        my @records;
        for ^$rows -> $row {
            my %record;
            for @wanted -> $name {
                my %column := %by-name{$name};
                my $values = %column<string_data> // %column<data> // [];
                if $row < $values.elems {
                    my $value = $values[$row];
                    my $domain = %column<domain>;
                    if $domain ~~ Positional &&
                            $value ~~ Numeric &&
                            $value == $value &&
                            0 <= $value.Int < $domain.elems {
                        $value = $domain[$value.Int];
                    }
                    %record{$name} = $value;
                }
            }
            @records.push(%record) if %record.elems;
        }
        @records
    }

    method head(UInt:D $rows = 10 --> Array) { self.preview(:$rows) }
    method tail(UInt:D $rows = 10 --> Array) {
        self.preview(:$rows, offset => (self.nrow - $rows max 0).UInt)
    }

    method expression(--> H2O::Client::Rapids::Expr) {
        H2O::Client::Rapids::Expr.frame($!client, $!id)
    }
    method select(*@columns) { self.expression.select(|@columns) }
    method where(H2O::Client::Rapids::Expr:D $predicate) {
        self.expression.where($predicate)
    }

    method summary(Str :$column) {
        my $base = "/3/Frames/{ $!client.encode($!id) }";
        $column.defined
            ?? $!client.get("$base/columns/{ $!client.encode($column) }/summary")
            !! $!client.get("$base/summary")
    }

    method download(IO::Path:D $path, *%query --> IO::Path:D) {
        my $bytes = $!client.get('/3/DownloadDataset', query => %(frame_id => $!id, |%query), :raw);
        $path.spurt($bytes, :bin);
        $path
    }

    method export(Str:D $server-path, *%options) {
        require H2O::Client::Job;
        my $response = $!client.post(
            "/3/Frames/{ $!client.encode($!id) }/export",
            content => %(path => $server-path, |%options));
        ::('H2O::Client::Job').from-response($!client, $response)
    }

    method delete() {
        $!client.delete("/3/Frames/{ $!client.encode($!id) }");
        $!deleted = True;
        $!loaded = False;
        True
    }

    method raw() { self.metadata }
    method gist() { "H2O::Frame<$!id>[{self.nrow} × {self.ncol}]" }
}
