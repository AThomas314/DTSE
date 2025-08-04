use polars::frame::column;
use polars::prelude::*;
use std::collections::HashMap;
mod utils;
use std::fs::File; // Required to create a file
use std::io::BufWriter;
use std::vec; // Recommended for performance when writing to files

fn main() {
    east_coast_grocers()
}

// fn auto(){
//     let whitespace_chars = " \t\n\r\x0C"; // Note: \x0C is form feed
//     let mut df = utils::load_data_from_csv("Session 1/autoshield_preproc.csv".into())
//         .lazy()
//         .with_columns([
//             col("claim_amount_usd").str().replace_all(whitespace_chars)
//         ])
//     ;   
// }

fn east_coast_grocers(){
        let mut df = utils::load_data_from_csv("Session 2/East Coast grocers beer sales.csv".into());
        df = utils::one_hot_encode(df,"brand2");
        println!("{:#?}",df)
}


fn zenith() {
    let whitespace_chars = " \t\n\r\x0C"; // Note: \x0C is form feed
    let mut df = utils::load_data_from_csv("Session 1/marketing_campaign_performance.csv".into())
        .lazy()
        .with_columns([
            col("impressions")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .cast(DataType::Int64), // .fill_null(lit(0)),
            col("clicks")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .cast(DataType::Int64), // .fill_null(lit(0)),
            col("conversions")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .cast(DataType::Int64), // .fill_null(lit(0)),
            col("spend_usd")
                .str()
                .replace(lit("$"), lit(""), true)
                .str()
                .replace_all(lit(" "), lit(""), true)
                .str()
                .replace_all(lit("USD"), lit(""), true)
                .cast(DataType::Float64), // .fill_null(lit(0.0)),
            col("revenue_usd")
                .str()
                .replace(lit("$"), lit(""), true)
                .str()
                .replace_all(lit(" "), lit(""), true)
                .str()
                .replace_all(lit("USD"), lit(""), true)
                .cast(DataType::Float64), // .fill_null(lit(0.0)),
            col("campaign_start_date")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .str()
                .to_date(StrptimeOptions {
                    format: Some("%d-%m-%Y".into()), // Specify the format here!
                    strict: false, // Set to true if you want parsing to fail on any malformed date.
                    exact: true,   // Set to true if you want the format to match exactly.
                    cache: true,   // Cache the format string for performance.
                }),
            col("campaign_end_date")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .str()
                .to_date(StrptimeOptions {
                    format: Some("%d-%m-%Y".into()), // Specify the format here!
                    strict: false, // Set to true if you want parsing to fail on any malformed date.
                    exact: true,   // Set to true if you want the format to match exactly.
                    cache: true,   // Cache the format string for performance.
                }),
            col("marketing_channel")
                .str()
                .to_lowercase()
                .str()
                .strip_chars(lit(whitespace_chars)),
            col("campaign_name")
                .str()
                .to_lowercase()
                .str()
                .strip_chars(lit(whitespace_chars)),
        ])
        .collect()
        .unwrap();

    println!("DataFrame \n {:#?}", &df);
    println!(
        "Dimensions of the DataFrame \n Rows : {:#?} \n Columns : {:#?}",
        &df.shape().0,
        &df.shape().1
    );
    let cols = &df.get_column_names();
    let types = &df.dtypes();
    let hm: HashMap<&&PlSmallStr, &DataType> = HashMap::from_iter(cols.iter().zip(types.iter()));
    println!("Types \n {:#?}", hm);
    println!(
        "Nulls \n{:#?}",
        df.null_count()
            .transpose(Some("Column Names"), None)
            .unwrap()
            .rename("column_0", "Null Counts".into())
            .unwrap()
            .sort_in_place(
                IntoVec::into_vec(vec![PlSmallStr::from_str("Null Counts")]),
                SortMultipleOptions {
                    descending: vec![true],
                    nulls_last: vec![true],
                    multithreaded: true,
                    maintain_order: true,
                    limit: None
                }
            )
            .unwrap()
    );
    let numeric_cols: Vec<PlSmallStr> = hm
        .iter()
        .filter(|(_k, v)| (v == &&&DataType::Float64) || (v == &&&DataType::Int64))
        .map(|(k, _v)| k.to_owned().to_owned().to_owned())
        .collect();
    println!("{:#?}", numeric_cols);
    let numeric_df = df.select(numeric_cols).unwrap();
    println!("{:#?}", numeric_df);
    for column in df.column_iter() {
        // println!("{:#?}",column);
        let dtype = column.dtype();
        println!("Name =>{:#?}", column.name());
        println!("Data Type =>{:#?}", dtype);
        println!("Unqiue Count =>{:#?}", column.n_unique().unwrap());
        println!("Null Count =>{:#?}", column.null_count());
        if (dtype == &DataType::Float64) || (dtype == &DataType::Int64) {
            println!(
                "Min =>{:#?}",
                column
                    .min_reduce()
                    .unwrap()
                    .value()
                    .to_string()
                    .parse::<f64>()
                    .unwrap()
            );
            println!(
                "Max =>{:#?}",
                column
                    .max_reduce()
                    .unwrap()
                    .value()
                    .to_string()
                    .parse::<f64>()
                    .unwrap()
            );
            println!(
                "Mean =>{:#?}",
                column
                    .mean_reduce()
                    .value()
                    .to_string()
                    .parse::<f64>()
                    .unwrap()
            );
            println!(
                "Median =>{:#?}",
                column
                    .median_reduce()
                    .unwrap()
                    .value()
                    .to_string()
                    .parse::<f64>()
                    .unwrap()
            );
        }
        if (dtype == &DataType::String) || (dtype == &DataType::Date) {
            println!(
                "{:#?}",
                column
                    .as_series()
                    .unwrap()
                    .value_counts(true, false, "".into(), false)
                    .unwrap()
                    .head(Some(10))
            );
        }
        println!("\n");
        println!("_________________________________________________");
        println!("\n");
    }
    df = df
        .lazy()
        .with_columns([
            (col("campaign_end_date") - col("campaign_start_date")).dt().total_days().alias("Campaign Duration"),
            (col("clicks") / col("impressions")).alias("Click-Through Rate"),
            (col("conversions") / col("clicks")).alias("Conversion Rate"),
            (col("spend_usd") / col("conversions")).alias("Customer Acquisition Cost"),
            (col("revenue_usd") / col("spend_usd")).alias("Return On Ad Spend"),
            (col("spend_usd").fill_null(col("spend_usd").median() - col("spend_usd").mean()))
                / col("spend_usd").std(0),
            (col("impressions").fill_null(col("impressions").median() - col("impressions").mean()))
                / col("impressions").std(0),
            (col("clicks").fill_null(col("clicks").median() - col("clicks").mean()))
                / col("clicks").std(0),
            (col("revenue_usd").fill_null(col("revenue_usd").median() - col("revenue_usd").mean()))
                / col("revenue_usd").std(0),
            (col("conversions").fill_null(col("conversions").median() - col("conversions").mean()))
                / col("conversions").std(0),
        ])
        .collect()
        .unwrap();
    println!("{:#?}", df);
    let file = File::create("Output.csv").expect("could not create output file");
    let mut writer = BufWriter::new(file);
    CsvWriter::new(&mut writer) // Pass a mutable reference to your writer
        .include_header(true) // Include column names as the first row
        .with_separator(b',') // Specify the separator (byte value for comma)
        .finish(&mut df).unwrap(); // Write the DataFrame. `finish` consumes the writer and the DataFrame.

}
