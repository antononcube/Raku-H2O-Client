use v6.d;

use H2O::Client::Exception;

class H2O::Client::Job {
    has $.client is required;
    has Str:D $.id is required;
    has Str $.destination-id;
    has Str $.destination-type;
    has Str $.status is rw = 'CREATED';
    has Numeric $.progress is rw = 0;
    has Str $.error is rw;
    has %!raw;

    method from-response(::?CLASS:U: $client, $response --> H2O::Client::Job) {
        my $job = $response<job> // ($response<jobs> // []).head // $response;
        my $id = $job<key><name> // $job<key> // $job<job_key><name>;
        die X::H2O::Job.new(message => 'H2O response does not contain a job key') unless $id.defined;
        self.new(:$client, :id($id.Str),
            destination-id => ($job<dest><name> // $job<dest> // $job<destination_key><name>),
            destination-type => ($job<dest><type> // $job<destination_key><type>),
            status => ($job<status> // 'CREATED').Str,
            progress => ($job<progress> // 0).Numeric)
    }

    method refresh(--> H2O::Client::Job) {
        my $response = $!client.get("/3/Jobs/{ $!client.encode($!id) }");
        my $job = ($response<jobs> // []).head // $response<job> // $response;
        %!raw = $job.Hash;
        $!status = ($job<status> // $!status).Str;
        $!progress = ($job<progress> // $!progress).Numeric;
        $!destination-id = $job<dest><name> // $!destination-id;
        $!destination-type = $job<dest><type> // $!destination-type;
        $!error = $job<exception> // $job<stacktrace> // $!error;
        self
    }

    method done(--> Bool:D) { $!status.uc eq 'DONE' }
    method failed(--> Bool:D) { so $!status.uc eq any(<FAILED CANCELLED STOPPED>) }

    method wait(Numeric:D :$timeout = 300, Numeric:D :$interval = 0.2 --> H2O::Client::Job) {
        my $deadline = now + $timeout;
        loop {
            self.refresh;
            die X::H2O::Job.new(message => "H2O job '$!id' failed: {$!error // $!status}") if self.failed;
            return self if self.done;
            die X::H2O::Job.new(message => "Timed out waiting for H2O job '$!id'") if now >= $deadline;
            sleep $interval;
        }
    }

    method cancel() {
        $!client.post("/3/Jobs/{ $!client.encode($!id) }/cancel");
        self.refresh
    }

    method result() {
        return Nil unless $!destination-id.defined;
        ($!destination-type // '') ~~ /:i frame/
            ?? $!client.frame($!destination-id)
            !! $!destination-id
    }

    method raw() { %!raw.Hash }
    method gist() { "H2O::Job<$!id>[$!status {round($!progress * 100)}%]" }
}
