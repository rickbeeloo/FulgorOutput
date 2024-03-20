use std::{write, io::{self, prelude::*, BufReader, BufWriter}};
use std::fs::File;
use clap::{Arg, App};
use std::collections::HashMap;
use indicatif::{ProgressBar, ProgressStyle};

fn parse_line<'a>(line: &'a str, suffix: &str) -> (u32, &'a str) {
    let index: u32 = line.split("\t").nth(0).unwrap().parse().expect("Could not parse index as u32");
    if line.contains("/") {
        let part = line.split("/").last().unwrap();
        return match part.strip_suffix(suffix) {
            Some(part) => (index, part),
            None => (index, part)
        };
    } 
    (index, line)
}

fn read_mfur_ids(file_path: &str, seq_suffix:&str) -> HashMap<u32, String> {
    let f = File::open(file_path).expect("File not found");
    let reader = BufReader::new(f);
    let mut start_parsing: bool = false;
    let mut index_to_id: HashMap<u32, String> = HashMap::new();
    for line in reader.lines() {
        let line = line.unwrap();

        // First few lines are comments, perhaps better to just skip
        // first n lines. In case the format changes this might be more reliable
        if line.chars().nth(0) == Some('0') {
            start_parsing = true;
        }

        if start_parsing {
            let (index, parsed_identifier) = parse_line(&line, &seq_suffix);
            index_to_id.insert(index, parsed_identifier.to_string());
        }
    }
    index_to_id
}

fn as_chunk_id(chunk_str: &str) -> u32 {
    chunk_str.split(":").next().unwrap().parse().expect("Failed to parse chunk id as u32")
}

fn extract_chunks(line: &str, identifier: &str, id_to_name: &HashMap<u32, String>, writer: &mut BufWriter<File>) {
    let stripped_line = line.trim_start_matches("chunk_id = ");

    // The first value is the chunk_id
    stripped_line.split(" ").enumerate().for_each(|(top_n, chunk)| {
        let chunk_id =  as_chunk_id(&chunk);

        // Get the sequence identifier for the given index
        match id_to_name.get(&chunk_id) {
            Some(sequence_id) => {
                write!(writer, "{}\t{}\t{}\n", identifier, top_n+1, sequence_id).unwrap();
            }
            None => {
                panic!("There is a sequence in the Fulgor file that is absent from the file names: {}", chunk_id);
            }
        }
    });
}

fn parse_fulgor_file(file_path: &str, id_to_name: &HashMap<u32, String>, output_name: &str) { 
    // Open for reading
    let f = File::open(file_path).expect("Fulgor file is missing");
    
    
    // Open writer and write header
    let mut writer = BufWriter::new(File::create(output_name).expect("Cant open output"));
    write!(writer, "query\ttop\tmatch\n").unwrap();

    let mut identifier = String::new();
    let file_size = f.metadata().unwrap().len();
    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .progress_chars("#>-"),
    );

    let reader = BufReader::new(f);
    for line in reader.lines() {
        let line = line.unwrap();
        // If we have a line starting with ">" we have a header tag
        if line.starts_with(">") {
            identifier = line.trim_start_matches(">").to_string();
        // We have chunk id line, that is chunk_id = X, [....]
        } else if line.starts_with("chunk_id") {
            extract_chunks(&line, &identifier, &id_to_name, &mut writer);
        }
        pb.inc(line.as_bytes().len() as u64);
    }

    pb.finish_with_message("Done parsing!");

}


fn main() {
    let matches = App::new("Fulgor topK output parser")
        .arg(Arg::with_name("fulgor")
            .help("Path to the fulgor output file")
            .required(true)
            .index(1))
        .arg(Arg::with_name("map")
            .help("Path to fulgor filename dump")
            .required(true)
            .index(2))
        .arg(Arg::with_name("output")
            .help("Path to output file")
            .required(true)
            .index(3))
        .arg(Arg::with_name("suffix")
            .help("Suffix to strip from file paths (DEFAULT: .fna)")
            .index(4)
            .default_value(".fna")) // Set default value here
        .get_matches();

    // Retrieve values from command line arguments
    let fulgor_file = matches.value_of("fulgor").expect("Provide fulgor input file");
    let filename_mapping_file = matches.value_of("map").expect("Provide mfur mapping file");
    let suffix = matches.value_of("suffix").unwrap_or(".fna");
    let output_file = matches.value_of("output").expect("Provide output file");

    // Call functions with command line arguments
    let id_2_name = read_mfur_ids(filename_mapping_file, suffix);
    println!("Running...");
    parse_fulgor_file(fulgor_file, &id_2_name, output_file);

}
