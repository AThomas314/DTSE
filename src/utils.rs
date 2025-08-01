use polars::prelude::*;

pub fn load_data_from_csv(file_path: String) -> DataFrame {
    let df = CsvReadOptions::default()
        .with_has_header(true)
        .try_into_reader_with_file_path(Some(file_path.into()))
        .unwrap()
        .finish()
        .unwrap();
    df
}

// fn describe(df : &DataFrame)->DataFrame{

// }
