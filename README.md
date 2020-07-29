---
graphlog
---

This utility creates a log of updates for the [GFE driver](https://github.com/cwida/gfe_driver). To reduce
the overhead in deciding which updates to execute at runtime, the GFE driver reads the log
prepared in advance by this tool, with the whole sequence of edge insertions and 
edge deletions that needs to be carried. 

The tool takes as input a graph in the 
[Graphalytics format](https://github.com/ldbc/ldbc_graphalytics) (.properties) 
and an _aging factor_,  and outputs a .graphlog file. In generating the updates, 
the tool respects the same node degree distribution of the input graph. For instance,
if the graph follows a power law distribution, then the updates will also 
follow the same power law distribution for the same vertices of the input graph. The aging factor
defines the number of updates created. Given an input graph with `E` edges, an aging factor 
`α` implies that about `α ⋅ E` updates will be generated.  The final output of the 
tool (.graphlog) is a compressed custom format meant only to be consumed by the GFE driver.

#### Requirements
- O.S. Linux
- CMake v 3.14 or newer
- A C++17 capable compiler

#### Fetch & build
```
git clone https://github.com/whatsthecraic/graphlog
cd graphlog
git submodule update --init
mkdir build && cd build
cmake ../ -DCMAKE_BUILD_TYPE=Release
make -j
```

The final artifact is the executable `graphlog`.

#### Usage

Type `./graphlog -h` for the full list of options. The basic pattern is:
```
./graphlog -a <aging_factor> -e 1 -v 1 /path/to/input/graph.properties output.graphlog
```

For example, to generate the same logs used in the experiments of the GFE driver, 
for [graph500-24](https://www.graphalytics.org/datasets) and 
[uniform-24](https://github.com/whatsthecraic/uniform_graph_generator), run:
```
./graphlog -a 10 -e 1 -v 1 /path/to/input/graph500-24.properties graph500-24-1.0.graphlog
./graphlog -a 10 -e 1 -v 1 /path/to/input/uniform-24.properties uniform-24-1.0.graphlog
```

Depending on the size of the graph, the tool could take several hours to complete. 