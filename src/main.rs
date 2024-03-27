use clap::{Arg, App};
use FulgorOutput::tabular::tab_parser::create_tabular;
use FulgorOutput::stats::stats::get_stats;
use clap::SubCommand;



fn main() {
        let matches = App::new("Fulgor topK output parser")
        .subcommand(
            SubCommand::with_name("tabular")
                .about("Process tabular data")
                .arg(
                    Arg::with_name("fulgor")
                        .help("Path to the fulgor output file")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("map")
                        .help("Path to fulgor filename dump")
                        .required(true)
                        .index(2),
                )
                .arg(
                    Arg::with_name("output")
                        .help("Path to output file")
                        .required(true)
                        .index(3),
                )
                .arg(
                    Arg::with_name("suffix")
                        .help("Suffix to strip from file paths (DEFAULT: .fna)")
                        .index(4)
                        .default_value(".fna"),
                ),
        )
        .subcommand(
            SubCommand::with_name("stats")
                .about("Generate statistics")
                .arg(
                    Arg::with_name("tabular_file")
                        .help("Path to the tabular file")
                        .required(true)
                        .index(1),
                )
                .arg(
                    Arg::with_name("chunk_annotation_file")
                        .help("Path to the annotation file")
                        .required(true)
                        .index(2),
                )
                .arg(
                    Arg::with_name("match_annotation_file")
                        .help("Path to the annotation file")
                        .required(true)
                        .index(3),
                )
                .arg(
                    Arg::with_name("output")
                        .help("Path to output file")
                        .required(true)
                        .index(4),
                ),
        )
        .get_matches();

    match matches.subcommand() {
        
        Some(("tabular", tabular_matches)) => {
            // Handle tabular subcommand
            let fulgor_file = tabular_matches.value_of("fulgor").expect("Provide fulgor input file");
            let filename_mapping_file = tabular_matches.value_of("map").expect("Provide mfur mapping file");
            let suffix = tabular_matches.value_of("suffix").unwrap_or(".fna");
            let output_file = tabular_matches.value_of("output").expect("Provide output file");
            create_tabular(&fulgor_file, &filename_mapping_file, &suffix, &output_file);
        }

        Some(("stats", stats_matches)) => {
            // Handle stats subcommand
            println!("Stats subcommand selected");
            let tab_file = stats_matches.value_of("tabular_file").expect("Tabular file missing");
            let chunk_anno_file = stats_matches.value_of("chunk_annotation_file").expect("Anno file missing");
            let match_anno_file = stats_matches.value_of("match_annotation_file").expect("Anno file missing");
            let output_file = stats_matches.value_of("output").expect("No output");

            get_stats(&tab_file, &chunk_anno_file, &match_anno_file, &output_file);
        }

        _ => {
            println!("No subcommand provided");
        }
    }
}


