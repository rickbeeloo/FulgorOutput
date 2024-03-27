

pub mod stats {

    use polars::prelude::*;
    use chrono::prelude::*;

    fn read_table(file_path: &str, schema: Schema) -> LazyFrame {
        return LazyCsvReader::new(file_path)
        .with_separator(b'\t')
        .with_schema(Some(schema.into()))
        .has_header(true)
        .finish()
        .expect("Failed to read csv");
    }

    fn read_chunk_annotation(file_path: &str) -> LazyFrame {
        // This is a chunk annotation, so we need two columns. 
        // one called "chunk" and another one "chunk_annotation"
        let schema = Schema::from_iter(vec![
            Field::new("query_genome_id", DataType::String),
            Field::new("query_contig_id", DataType::String),
            Field::new("chunk", DataType::UInt64),
            Field::new("chunk_annotation", DataType::String)
        ]);
        return read_table(&file_path, schema);
    }

    fn read_match_annotation(file_path: &str) -> LazyFrame {
        // This is a chunk annotation, so we need two columns. 
        // one called "chunk" and another one "chunk_annotation"
        let schema = Schema::from_iter(vec![
            Field::new("match_genome_id", DataType::String),
            Field::new("match_annotation", DataType::String)
        ]);
        return read_table(&file_path, schema);
    }

    fn read_fulgor_table(tabular_path: &str) -> LazyFrame {
        // Define the data scheme
        let schema = Schema::from_iter(vec![
            Field::new("query", DataType::String), // query is a concatenation of genome_id&contig_id
            Field::new("chunk", DataType::UInt64),
            Field::new("top", DataType::UInt64),
            Field::new("match_genome_id", DataType::String) // We have to make sure to get genome ids as strings -  always
        ]);

        return read_table(&tabular_path, schema);
    }

    fn add_chunk_metadata(fulgor_table: LazyFrame, chunk_table: LazyFrame) -> LazyFrame {
        let comb_table = fulgor_table
        .with_columns([
            col("query")
            .str()
            .split_exact(lit("_"),1)
            .struct_()
            .rename_fields(["query_genome_id".into(), "query_contig_id".into()].to_vec())
            ])
        .unnest(["query"])
        .join(
            chunk_table,
            [col("query_genome_id"), col("query_contig_id"), col("chunk")],
            [col("query_genome_id"), col("query_contig_id"), col("chunk")],
            JoinArgs::new(JoinType::Left),
        );
        comb_table
    }

    fn get_query_counts(comb_table: &LazyFrame, sample_size: u32) -> LazyFrame {
        let query_sample_table = comb_table.clone()
        .filter(
            col("chunk_annotation").is_not_null()
        ) 
        .group_by(["query_genome_id"])
        .agg([(len() * lit(sample_size)).alias("sample_target_size")]); //basically we get the number of negatives to sample
        return query_sample_table
    }

    fn count_match_annotations(table: LazyFrame, match_table: LazyFrame, col_name: &str) -> LazyFrame {
        let count_table = table
        .join(
            match_table.clone(),
            [col("match_genome_id")],
            [col("match_genome_id")],
            JoinArgs::new(JoinType::Left),
        )
        .group_by(["match_annotation"])
        .agg(
            [
                (len()).alias(col_name), 
            ])
        .with_columns([
            (
                col(col_name).cast(DataType::Float32) 
                /
                col(col_name).sum().cast(DataType::Float32)
            ).alias(col_name)
            ])
            ;
        return count_table;
    }

    fn get_positive_set(comb_table: LazyFrame, match_table: LazyFrame) -> LazyFrame {
        let positives = comb_table
            .clone()
            .filter(
                col("chunk_annotation").is_not_null()
            ); 
        let positives = count_match_annotations(positives, match_table, "pos_count");
        return positives;
    }

    fn get_negative_set(comb_table: LazyFrame, match_table: LazyFrame, sample_table: LazyFrame) -> LazyFrame {
        let negatives = comb_table
            .filter(
                col("chunk_annotation").is_null()
            )
            .join(
                sample_table,
                [col("query_genome_id")],
                [col("query_genome_id")],
                JoinArgs::new(JoinType::Left),
            )
            .filter(
                int_range(lit(0), len(), 1, DataType::UInt64)
                .shuffle(Some(12345)) // random seed
                .over(["query_genome_id"])
                .lt(col("sample_target_size").max()) // Sample at most X, NOTE sample could therefore be less than expected
            );
        let negatives = count_match_annotations(negatives, match_table, "neg_count");
        return negatives;
    }

    fn calc_fold_change(positives: LazyFrame, negatives: LazyFrame) -> LazyFrame {
        let result = positives
            .join(
                negatives,
                [col("match_annotation")],
                [col("match_annotation")],
                JoinArgs::new(JoinType::Left),
            )
            .with_columns([
                (
                    ( 
                        (
                            col("pos_count").cast(DataType::Float32)
                            / 
                            col("neg_count").cast(DataType::Float32)
                        )
                        .log(2.0)
                    )
                ).alias("fold_change") // Normalize against sample size
            ]
            ).sort(
                "fold_change",
                SortOptions {
                    descending: true,
                    nulls_last: true,
                    ..Default::default()
                },
            );
        return result;
    }

    fn process_genomes(fulgor_table: LazyFrame, chunk_table: LazyFrame, match_table: LazyFrame) -> DataFrame {
        let local: DateTime<Local> = Local::now();
        println!("{} Started calculations..", local);
        
        let sample_size: u32 = 100;

        // Add chunk metadata to the fulgor table
        let comb_table = add_chunk_metadata(fulgor_table, chunk_table);

        // Make a table to store the AMR count per genome
        let query_sample_table = get_query_counts(&comb_table, sample_size);

        // Get positive and negative set
        // P.s clones are cheap, we only copy the exectuion plan as these are Lazy dataframe (so not the data)
        let positives = get_positive_set(comb_table.clone(), match_table.clone());
        let negatives = get_negative_set(comb_table.clone(), match_table.clone(), query_sample_table); 

        // Calculate the fold changes
        let fold_table = calc_fold_change(positives, negatives);
        return fold_table.collect().expect("Failed to calculate fold changes");
    }

    pub fn get_stats(fulgor_file_path: &str, chunk_anno_path: &str, match_anno_path: &str, output_path: &str) {
        let fulgor_table = read_fulgor_table(&fulgor_file_path);
        let chunk_table = read_chunk_annotation(&chunk_anno_path);
        let match_table = read_match_annotation(&match_anno_path);
        let mut fold_table = process_genomes(fulgor_table, chunk_table, match_table);
        println!("Table (head 10): {:?}", fold_table.head(Some(10)));

        let mut file = std::fs::File::create(output_path).unwrap();

        let local: DateTime<Local> = Local::now();
        println!("{} Saving to file..", local);
        
        CsvWriter::new(&mut file).finish(&mut fold_table).unwrap();

        let local: DateTime<Local> = Local::now();
        println!("{} Done!..", local);
    }


}