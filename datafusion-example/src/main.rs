use datafusion::prelude::*;
use datafusion_substrait::{logical_plan::consumer::from_substrait_plan, substrait::proto::Plan};
use std::{error::Error, fs::File, io::BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ctx = SessionContext::new();

    ctx.register_parquet(
        "penguins",
        "../data/penguins.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    let plan = serde_json::from_reader::<_, Plan>(BufReader::new(
        File::open("../tools/plan.json").expect("file not found"),
    ))
    .expect("Failed to read plan from disk.");

    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    let new_df = ctx.execute_logical_plan(logical_plan).await?;

    let _ = new_df.show().await;

    Ok(())
}
