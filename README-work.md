# Raku-H2O-Client


[![MacOS](https://github.com/antononcube/Raku-H2O-Client/actions/workflows/macos.yml/badge.svg)](https://github.com/antononcube/Raku-H2O-Client/actions/workflows/macos.yml)
[![Linux](https://github.com/antononcube/Raku-H2O-Client/actions/workflows/linux.yml/badge.svg)](https://github.com/antononcube/Raku-H2O-Client/actions/workflows/linux.yml)
[![Win64](https://github.com/antononcube/Raku-H2O-Client/actions/workflows/windows.yml/badge.svg)](https://github.com/antononcube/Raku-H2O-Client/actions/workflows/windows.yml)

Raku REST client for the open-source, distributed in-memory machine-learning platform [H2O-3](https://h2o.ai/).

---

## Installation

From Zef ecosystem:

```
zef install H20::Client
```

From GitHub:

```
zef install https://github.com/antononcube/Raku-H2O-Client.git
```

---

## Setup

**1.** Download the latest [H20 version](http://h2o-release.s3.amazonaws.com/h2o/rel-3.46.0/12/index.html).

**2.** Start in a OS-terminal application the H2O cluster with the shell command:

```
java -jar h2o.jar
```

**3.** In case that command give the message:

> Only Java versions 8-17 are supported, system version is 22.0.2

**3.1.** Check the available Java distributions with:

```
/usr/libexec/java_home -V
```

**3.2.** Pick one with a version between 8-17 or download a one with one of these versions:

**3.3.** Setup `JAVA_HOME` and run the command `java -jar h2o.jar` again. For example, on macOS:

```
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home 
java -jar h2o.jar 
```

----

## Usage examples

```raku
use H2O::Client;

my $h2o = H2O::Client.new('http://localhost:54321');

my $frame = $h2o.upload(
    [%(x => 1, group => 'a'), %(x => 2, group => 'b')],
    destination-frame => 'example.hex'
).wait.result;

say $frame.shape;       # (2 2)
say $frame.names;
say $frame<group>.type; # column proxy
say $frame.head(2);     # explicitly download a small preview
```

`H2O::Client::Frame` is a lightweight handle to a server-side frame. Its
identity is the H2O frame key; metadata is fetched lazily and cached. Use
`refresh` after external changes. `H2O::Client::Column` exposes column type,
domain, and summary statistics. Long-running parse, model-build, and export
operations return `H2O::Client::Job`; call `wait` before consuming `result`.

## Rapids transformations

Frame transformations are explicit, composable Rapids expressions. They do not
run until `materialize` is called with a destination key:

```raku
use Data::ExampleDatasets;

my @data = example-dataset(/'dplyr::starwars'/);
$h2o.upload(@data, destination-frame => 'starwars');

my $frame = $h2o.frame('starwars');
my $heavy = $frame
        .where($frame<mass>.expression.greater-than(60))
        .select(<name mass homeworld>)
        .materialize('starwars-heavy3.hex');
```

The initial layer supports column selection, row filtering, arithmetic,
comparisons, boolean operations, `cbind`, `rbind`, `sum`, and `mean`. It opens
an H2O Rapids session lazily; call `$h2o.close` when retaining a connection for
a long-running process is no longer necessary.

To start a local cluster:

```raku, eval=FALSE
my $h2o = H2O::Client.new;
$h2o.init(jar-path => '/path/to/h2o.jar', jvm-opts => <-Xmx4g>);
LEAVE $h2o.shutdown;
```

`shutdown` stops only a process started by this client. Shutting down a cluster
connected to externally requires the explicit `:cluster` option.

---

## Workflows 

For fully programmed workflows see the example Raku scripts:

- [classification.raku](https://github.com/antononcube/Raku-H2O-Client/blob/main/examples/classification.raku)
- [clustering.raku](https://github.com/antononcube/Raku-H2O-Client/blob/main/examples/clustering.raku)

Or the Jupyter notebooks:

- [Classification-demo.ipynb](https://github.com/antononcube/Raku-H2O-Client/blob/main/docs/Classification-demo.ipynb)
- [Data-manipulation-demo.ipynb](https://github.com/antononcube/Raku-H2O-Client/blob/main/docs/Data-manipulation-demo.ipynb)


---

## References

### Documentation, downloads

[H2O1] H20.ai, [H2O-3 Documentation](https://docs.h2o.ai/h2o-3).

[H2O2] H20.ai, [H20 Latest Stable Release](https://h2o-release.s3.amazonaws.com/h2o/latest_stable.html).

### Packages

[AAp1] Anton Antonov, [Data::ExampleDatasets, Raku package](https://github.com/antononcube/Raku-Data-ExampleDatasets), (2021-2025), [GitHub/antononcube](https://github.com/antononcube). 

[AAp2] Anton Antonov, [Data::Importers, Raku package](https://github.com/antononcube/Raku-Data-Importers), (2024-2026), [GitHub/antononcube](https://github.com/antononcube). 