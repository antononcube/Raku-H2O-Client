#!/usr/bin/env raku
use v6.d;

use H2O::Client;

use Data::Reshapers;
use Data::Summarizers;
use Data::ExampleDatasets;

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
#@dsExample .= map({ $_<Age> = (10 * round($_<Age> / 10)).Str; $_ });

records-summary(@dsExample);

#`[
# Export example dataset as a CSV file
my $filePath = $*TMPDIR ~ '/titanic.csv';
csv( in => @dsExample, out => $filePath, sep => ',');

# Import dataset in H2O
my %importRes = $h2o.data-import($filePath);
]

#----------------------------------------------------------
# Make training and testing datasets
#----------------------------------------------------------

my (@training-indexes, @testing-indexes);
with take-drop((^@dsExample.elems).pick(*), floor(@dsExample.elems * 0.75) ) {
        @training-indexes = $_.head;
        @testing-indexes = $_.tail;
}

my @field-names = <Age Sex PClass Survived>;

my @dsTraining = select-columns(@dsExample[@training-indexes], @field-names);
my @dsTesting = select-columns(@dsExample[@testing-indexes], @field-names);

say dimensions(@dsTraining);
say dimensions(@dsTesting);

say "Training:";
records-summary(@dsTraining, :@field-names);

say "Testing:";
records-summary(@dsTesting, :@field-names);

# Upload, parse, and wait for both remote frames.
my $training-frame = $h2o.upload(
    @dsTraining,
    destination-frame => 'titanic-training.hex',
    column-names => @field-names,
    column-types => <Enum Enum Enum Enum>
).wait.result;

my $testing-frame = $h2o.upload(
    @dsTesting,
    destination-frame => 'titanic-testing.hex',
    column-names => @field-names,
    column-types => <Enum Enum Enum Enum>
).wait.result;

say "Training frame: {$training-frame.gist}";
say "Testing frame: {$testing-frame.gist}";

say "Frames : ";
my @dsFrames = $h2o.frames('summary');
say to-pretty-table(@dsFrames);

#==========================================================
# Poll jobs
#==========================================================

my @dsJobs = $h2o.jobs('summary');
say to-pretty-table(@dsJobs);


#==========================================================
# Model build and predict
#==========================================================

my %model-props =
        response_column => @field-names.tail,
        training_frame => "titanic-training.hex"
        ;

my $model-job = $h2o.model-build('drf', %model-props).wait;

say "Models :";
my @dsModels = $h2o.models('summary');

say to-pretty-table(@dsModels);

my $model-id = $model-job.destination-id // @dsModels.tail<id>;
say "Using : {(:$model-id)}";

my $predictions = $h2o.model-predict(
    $model-id, $testing-frame.id, 'titanic-predictions.hex');

say 'Prediction result';
say $predictions.gist;

say 'Frames';
.&to-pretty-table.say for $h2o.frames;
