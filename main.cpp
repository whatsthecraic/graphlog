#include <cassert>
#include <iostream>
#include <mutex>
#include <random>
#include <string>

#include "lib/common/error.hpp"
#include "lib/common/filesystem.hpp"
#include "lib/common/system.hpp"
#include "lib/common/timer.hpp"
#include "lib/cxxopts.hpp"

#include "generator.hpp"
#include "writer.hpp"

using namespace common;
using namespace std;

// globals
double g_aging = 10.0; // number of operations to perform, w.r.t. to the size of the loaded graph
double g_ef_edges = 1.0; // expansion factor for the edges in the graph
double g_ef_vertices = 1.2; // expansion factor for the vertices in the graph
string g_path_input; // path to the input graph, in the Graphalytics format
string g_path_output; // path where to store the log of updates
uint64_t g_seed = std::random_device{}(); // the seed to use for the random generator

// logging
mutex g_mutex_log;

// function prototypes
static void parse_command_line_arguments(int argc, char* argv[]);
static uint64_t num_operations(); // total number of operations to produce


int main(int argc, char* argv[]) {
    Timer timer; timer.start();

    try {
        parse_command_line_arguments(argc, argv);

        Writer writer;
        writer.set_property("aging_coeff", g_aging);
        writer.set_property("ef_edges", g_ef_edges);
        writer.set_property("ef_vertices", g_ef_vertices);
        writer.set_property("git_last_commit", common::git_last_commit());
        writer.set_property("hostname", common::hostname());
        writer.set_property("input_graph", g_path_input);
        writer.set_property("seed", g_seed);

        Generator generator {g_path_input, g_path_output, writer, 1.0, g_ef_vertices, g_ef_edges, g_aging, g_seed};
        generator.generate();
    } catch (common::Error& e){
        cerr << e << endl;
        cerr << "Type `" << argv[0] << " --help' to check how to run the program\n";
        cerr << "Program terminated" << endl;
        return 1;
    }

    cout << "\nWhole completion time " << timer << "\n";

    return 0;
}


static void parse_command_line_arguments(int argc, char* argv[]){
    using namespace cxxopts;

    Options options(argv[0], "Graph Generator of Updates (graphlog): create a log of edge updates based on the distribution of the input graph");
    options.custom_help(" [options] <input> <output>");
    options.add_options()
        ("a, aging", "Number of operations to produce w.r.t. the size of the loaded graph", value<double>()->default_value(to_string(g_aging)))
        ("e, efe", "Expansion factor for the edges in the graph", value<double>()->default_value(to_string(g_ef_edges)))
        ("v, efv", "Expansion factor for the vertices in the graph", value<double>()->default_value(to_string(g_ef_vertices)))
        ("h, help", "Show this help menu")
        ("seed", "Seed to initialise the random generator", value<uint64_t>())
    ;

    auto parsed_args = options.parse(argc, argv);

    if( argc == 1 || parsed_args.count("help") > 0 ){
        cout << options.help() << endl;
        exit(EXIT_SUCCESS);
    }

    if( argc != 3 ) {
        INVALID_ARGUMENT("Invalid number of arguments: " << argc << ". Expected format: " << argv[0] << " [options] <input> <output>");
    }
    if(!common::filesystem::file_exists(argv[1])){
        INVALID_ARGUMENT("The given input graph does not exist: `" << argv[1] << "'");
    }
    g_path_input = argv[1];
    g_path_output = argv[2];

    if(parsed_args.count("aging") > 0){
        double value = parsed_args["aging"].as<double>();
        if(value < 1.0){
            INVALID_ARGUMENT("The expansion factor must be a value equal or greater than 1: " << value);
        }
        g_aging = value;
    }

    if(parsed_args.count("efv") > 0){
        double value = parsed_args["efv"].as<double>();
        if(value < 1.0){
            INVALID_ARGUMENT("The expansion factor must be a value equal or greater than 1: " << value);
        }
        g_ef_vertices = value;
    }

    if(parsed_args.count("efe") > 0){
        double value = parsed_args["efe"].as<double>();
        if(value < 1.0){
            INVALID_ARGUMENT("The expansion factor must be a value equal or greater than 1: " << value);
        }
        g_ef_edges = value;
    }

    if(parsed_args.count("seed") > 0){
        g_seed = parsed_args["seed"].as<uint64_t>();
    }

    cout << "Path input graph: " << g_path_input << "\n";
    cout << "Path output log: " << g_path_output << "\n";
    cout << "Aging factor: " << g_aging << "\n";
    cout << "Expansion factor for the vertices: " << g_ef_vertices << "\n";
    cout << "Expansion factor for the edges: " << g_ef_edges << "\n";
    cout << "Seed for the random generator: " << g_seed << "\n";
    cout << endl;
}