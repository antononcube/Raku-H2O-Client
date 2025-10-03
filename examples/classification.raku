#!/usr/bin/env raku
use v6.d;

use lib <. lib>;

use JSON::Fast;
use HTTP::Tiny;
use URI::Encode;
use H2O::Client;

use Data::Reshapers;
use Data::Summarizers;
use Data::ExampleDatasets;
use Text::CSV;

#==========================================================
# H2O cluster access
#==========================================================
my $base-url = 'http://127.0.0.1:54321';

my $h2o = H2O::Client.new($base-url, tz => $*TZ);

#==========================================================
# Data
#==========================================================
my @dsExample = example-dataset('Stat2Data::Titanic');
#@dsExample .= map({ $_<Survived> = $_<Survived> ?? 'yes' !! 'no'; $_ });

records-summary(@dsExample);

# Export example dataset as a CSV file
my $filePath = $*TMPDIR ~ '/titanic.csv';
csv( in => @dsExample, out => $filePath, sep => ',');

# Import dataset in H2O
my %importRes = $h2o.data-import($filePath);

# Get parsing analysis/setup
my %parseSetupRes = $h2o.data-parse-setup([%importRes<destination_frames>]);

my %content =
        destination_frame => 'titanic.hex',
        source_frames => [%parseSetupRes<source_frames>.head<name>],
        parse_type => %parseSetupRes<parse_type>,
        separator => %parseSetupRes<separator>,
        number_columns => %parseSetupRes<number_columns>,
        single_quotes => False,
        column_names => @dsExample.head.keys,
        column_types => %parseSetupRes<column_types>,
        check_header => -1,
        delete_on_done => True,
        chunk_size => 4194304
        ;

my %parseRes = $h2o.data-parse(%content);

#==========================================================
# Poll jobs
#==========================================================

my @dsJobs = $h2o.jobs('summary');

say to-pretty-table(@dsJobs);


#==========================================================
# Model build and predict
#==========================================================

my %model-props =
        "training_frame" => "titanic.hex",
        "response_column" => "Survived";

my %modelRes = $h2o.model-build('drf', %model-props);

say (:%modelRes);

my @dsModels = $h2o.models('summary');

say to-pretty-table(@dsModels);

my $model-id = @dsModels.tail<name>;
my %predRes = $h2o.model-predict($model-id, 'titanic.hex', 'titanic-predictions.hex');

say 'Prediction reuslt';
say to-json(%predRes);

say 'Frames';
say to-pretty-table($h2o.frames);
