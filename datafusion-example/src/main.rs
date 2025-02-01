use datafusion::prelude::*;
use datafusion_substrait::{logical_plan::consumer::from_substrait_plan, serializer};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let ctx = SessionContext::new();

    ctx.register_parquet(
        "penguins",
        "../data/penguins.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    let plan: Box<datafusion_substrait::substrait::proto::Plan> =
        serializer::deserialize("../tools/plan.bin").await?;
    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    let new_df = ctx.execute_logical_plan(logical_plan).await?;

    let _ = new_df.show().await;

    Ok(())
}
