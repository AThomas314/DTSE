use polars::prelude::*;
use std::collections::HashMap;
mod utils;

fn main() {
    let whitespace_chars = " \t\n\r\x0C"; // Note: \x0C is form feed
    let mut df = utils::load_data_from_csv("Session 1/marketing_campaign_performance.csv".into())
        .lazy()
        .with_columns([
            col("impressions")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .cast(DataType::Int64)
                // .fill_null(lit(0)),
                ,
            col("clicks")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .cast(DataType::Int64)
                // .fill_null(lit(0)),
                ,
            col("conversions")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .cast(DataType::Int64)
                // .fill_null(lit(0)),
                ,
            col("spend_usd")
                .str()
                .replace(lit("$"), lit(""), true)
                .str()
                .replace_all(lit(" "), lit(""), true)
                .str()
                .replace_all(lit("USD"), lit(""), true)
                .cast(DataType::Float64)
                // .fill_null(lit(0.0)),
                ,
            col("revenue_usd")
                .str()
                .replace(lit("$"), lit(""), true)
                .str()
                .replace_all(lit(" "), lit(""), true)
                .str()
                .replace_all(lit("USD"), lit(""), true)
                .cast(DataType::Float64)
                // .fill_null(lit(0.0)),
                ,
            col("campaign_start_date")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .cast(DataType::Date),
            col("campaign_end_date")
                .str()
                .replace_all(lit(" "), lit(""), true)
                .cast(DataType::Date),
            col("marketing_channel")
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
    println!("Types {:#?}",hm);
    println!("Nulls {:#?}",&df.null_count())
}
// fn main() {
//     let mut df = utils::load_data_from_csv("Session 1/autoshield_preproc.csv".into());
//     println!("DataFrame \n {:#?}", &df);
//     let na_patterns = r"(?i)\s*(?:NA|N/A|NaN|null|^$)\s*";
//     let columns: &Vec<String> = &df
//         .get_column_names()
//         .iter()
//         .map(|x| x.to_string())
//         .collect();
//     let dtypes: &Vec<String> = &df.dtypes().iter().map(|x| x.to_string()).collect();
//     let mut string_columns: Vec<String> = Vec::new();
//     println!("DataFrame data types {:#?}", dtypes);
//     let mut i = 0;
//     while i < columns.len() {
//         if dtypes[i] == "str".to_string() {
//             string_columns.push(columns.get(i).unwrap().to_owned());
//         } else {
//             println!("{:#?}", dtypes[i]);
//         }
//         i += 1;
//     }
//     println!("STRING COLUMNS {:#?}", &string_columns);
//     println!("ALL COLUMNS {:#?}", &columns);
//     df = df
//         .lazy()
//         .with_columns([col("date_of_incident").cast(DataType::Date)])
//         .collect()
//         .unwrap()

// }
