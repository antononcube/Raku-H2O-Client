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

1. Download the latest [H20 version](http://h2o-release.s3.amazonaws.com/h2o/rel-3.46.0/12/index.html).

2. Start in a OS-terminal application the H2O cluster with the shell command:

```
java -jar h2o.jar
```

3. In case that command give the message:

> Only Java versions 8-17 are supported, system version is 22.0.2

3.1. Check the available Java distributions with:

```
/usr/libexec/java_home -V
```

3.2. Pick one with a version between 8-17 or download a one with one of these versions:

3.3. Setup `JAVA_HOME` and run the command `java -jar h2o.jar` again. For example, on macOS:

```
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-11.jdk/Contents/Home 
java -jar h2o.jar 
```

## Usage examples

```raku
use H2O::Client;

my $h2o = H2O::Client.new('http://127.0.0.1:54321');

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
my $adults = $frame
    .where($frame<age>.expression.greater-than(18))
    .select(<age income>)
    .materialize('adults.hex');
```

The initial layer supports column selection, row filtering, arithmetic,
comparisons, boolean operations, `cbind`, `rbind`, `sum`, and `mean`. It opens
an H2O Rapids session lazily; call `$h2o.close` when retaining a connection for
a long-running process is no longer necessary.

To start a local cluster:

```raku
my $h2o = H2O::Client.new;
$h2o.init(jar-path => '/path/to/h2o.jar', jvm-opts => <-Xmx4g>);
LEAVE $h2o.shutdown;
```

`shutdown` stops only a process started by this client. Shutting down a cluster
connected to externally requires the explicit `:cluster` option.

----

## Tests

Fast tests use mocked transport responses and do not require Java:

```console
prove6 -Ilib t
```

Live integration tests are intentionally kept under `xt/`:

```console
H2O_JAR=/path/to/h2o.jar prove6 -Ilib xt
```

H2O 3.46 supports Java 8–17. If `java` on `PATH` is newer, set `H2O_JAVA`
to the full path of a supported Java executable.