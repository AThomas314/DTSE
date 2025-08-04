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
pub fn one_hot_encode(mut df:DataFrame,column_name:&str)->DataFrame{

        let column = df.column(column_name).unwrap();
        let all_items :Vec<String> = column.str().into_iter().flatten().map(|x|{x.unwrap().to_string()}).collect();
        let mut unique_items = all_items.clone();
        unique_items.sort();
        unique_items.dedup();
        let mut cols:Vec<column::Column> = Vec::with_capacity(unique_items.len()-1);
        let mut i =0;
        while i< unique_items.len()-1 {
            let unique_item = &unique_items.get(i).unwrap();
            let items_vec: Vec<i64> = all_items.iter().map(|x|{if &x==unique_item{
                1
            } else {0
            }}).collect();
            let col = column::Column::from(Series::new(PlSmallStr::from_string(unique_item.to_owned().to_owned()), items_vec));
            cols.push(col);

            i+=1;
        }
        df = df.hstack(&cols).unwrap();
        df.drop(column_name).unwrap()
}

// fn describe(df : &DataFrame)->DataFrame{

// }
