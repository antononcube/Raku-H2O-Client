#!/usr/bin/env raku
use v6.d;

use H2O::Client;

use Data::Reshapers;
use Data::Summarizers;
use Data::ExampleDatasets;

#==========================================================
# H2O cluster access
#==========================================================
my $base-url = 'http://localhost:54321';

my $h2o = H2O::Client.new($base-url, tz => $*TZ);

#==========================================================
# Data
#==========================================================
my @dsExample = example-dataset('Stat2Data::Titanic');

records-summary(@dsExample);

#----------------------------------------------------------
# Make the training dataset
#----------------------------------------------------------


my @field-names = <Age Sex PClass Survived>;

my @dsTraining = select-columns(@dsExample, @field-names);

# Make numeric (factorize) the columns of interest ("Age" and "Survived" are numerical already)
@dsTraining = @dsTraining.map({
    $_<PClass> = $_<PClass> ~~ /^ \d / ?? $_<PClass>.substr(0,1).Int !! -1;
    $_<Sex> = $_<Sex> eq 'female' ?? 0 !! 1;
    $_});

.say for @dsTraining.head(10);

say 'dimensions(@dsTraining) => ', dimensions(@dsTraining);

say "Training:";
records-summary(@dsTraining, :@field-names);

# Upload, parse, and wait for the training frame.
my $training-frame = $h2o.upload(
    @dsTraining,
    destination-frame => 'titanic-training.hex',
    column-names => @field-names,
    column-types => <Int Int Int Int>
).wait.result;

say "Training frame: {$training-frame.gist}";

# Materialize every training column as a categorical factor.  `as-factor`
# operates on one column, so combine the resulting expressions into the frame
# that will be supplied to K-means.
my $prefix = "raku-kmeans-{$*PID}";
#my @factor-columns = @field-names.map({ $training-frame[$_].as-factor });
#my $factorized-training-frame = @factor-columns.head
#    .cbind(|@factor-columns.skip(1))
#    .materialize("{$prefix}-training-factors.hex");

say "Factorized training frame: {$training-frame.gist}";

say "Frames : ";
my @dsFrames = $h2o.frames('summary');
say to-pretty-table(@dsFrames);

#==========================================================
# Poll jobs
#==========================================================

say '=' x 100;
say 'Jobs';
my @dsJobs = $h2o.jobs('summary');
say to-pretty-table(@dsJobs);


#==========================================================
# Model build and predict
#==========================================================

# K-means is an unsupervised model: do not provide response_column.
# estimate_k asks H2O K-means to select the number of clusters iteratively.
# k is consequently a maximum, rather than a fixed cluster count.

my $model-job = $h2o.model-build('kmeans', %(
    model_id => "{$prefix}-model",
    training_frame => $training-frame.id,
    estimate_k => True,
    k => 6,
    standardize => True,
    max_iterations => 100,
    seed => 42,
)).wait;

my $model-id = $model-job.destination-id // "{$prefix}-model";
say "K-means model: $model-id";

# Score the input frame.  The prediction frame contains H2O's cluster
# assignment for every row (and distance columns, depending on H2O version).
my $predictions = $h2o.model-predict(
        $model-id, $training-frame.id, "{$prefix}-predictions.hex");

say 'Cluster assignments sample:';
.say for $predictions.preview;

say 'Number of items per cluster:';
say $predictions.Array.map(*<predict>).Bag;

#====================================================================================================
say '=' x 100;
say "Models :";
my @dsModels = $h2o.models('summary');

.say for @dsModels;
