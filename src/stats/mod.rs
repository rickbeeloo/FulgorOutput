

pub mod stats {

    use polars::prelude::*;

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

    fn process_genomes(fulgor_table: LazyFrame, chunk_table: LazyFrame, match_table: LazyFrame) -> DataFrame {
        let sample_size = 10;

        // This is quite some memory if we join it all together, perhaps split up later, todo
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
            match_table,
            [col("match_genome_id")],
            [col("match_genome_id")],
            JoinArgs::new(JoinType::Left),
        )
        .join(
            chunk_table,
            [col("query_genome_id"), col("query_contig_id"), col("chunk")],
            [col("query_genome_id"), col("query_contig_id"), col("chunk")],
            JoinArgs::new(JoinType::Left),
        );

        println!("Come table: {:?}", comb_table.clone().collect());
        

        let positives = comb_table.clone()
        .filter(
            col("chunk_annotation").is_not_null()
        ) 
        .group_by(["match_annotation"])
        .agg([len().alias("pos_count")]);

        // Randomly sample 10 per genome
        let negative_samples = comb_table.clone()
        .filter(
            col("chunk_annotation").is_null()
        )
        .filter(
            int_range(lit(0), len(), 1, DataType::UInt64)
            .shuffle(Some(12345)) // random seed
            .over(["match_genome_id"])
            .lt(sample_size)
        )
        .group_by(["match_annotation"])
        .agg([len().alias("neg_count")]);


        // Combine positive and negative samples
        // For each psoitive we want to know the negative
        // also other way around?
        return positives
        .join(
            negative_samples,
            [col("match_annotation")],
            [col("match_annotation")],
            JoinArgs::new(JoinType::Left),
        )
        .with_columns([
            (
                ((col("pos_count").cast(DataType::Float32) /  ((col("neg_count").cast(DataType::Float32) / lit(sample_size)))).log(2.0)
            )).alias("fold_change") // Normalize against sample size
        ]
        ).sort(
            "fold_change",
            SortOptions {
                descending: true,
                nulls_last: true,
                ..Default::default()
            },
        )
        .collect().expect("Failed to calculate fold changes");
    }

    pub fn get_stats(fulgor_file_path: &str, chunk_anno_path: &str, match_anno_path: &str) {
        let fulgor_table = read_fulgor_table(&fulgor_file_path);
        println!("Fulgor table: {:?}", fulgor_table.clone().collect());
        let chunk_table = read_chunk_annotation(&chunk_anno_path);
        println!("Chunk table: {:?}", chunk_table.clone().collect());
        let match_table = read_match_annotation(&match_anno_path);
        println!("Chunk table: {:?}", match_table.clone().collect());
        let fold_table = process_genomes(fulgor_table, chunk_table, match_table);
        println!("fold table: {:?}", fold_table);

    }


}