# Fulgor parser & stats

Simple code to convert the Fulgor (dev) output to a tabular file (`tabular`). This tabular file can then be used to get simple statistics (`stats`), such as the fold-change between two groups. 

The file processing is written using [Polars](https://docs.rs/polars/latest/polars/) making it relatively easy to change columns and calculations without much knowledge of Rust. See [Polars docs](https://docs.pola.rs/user-guide/getting-started/) for more detail. 