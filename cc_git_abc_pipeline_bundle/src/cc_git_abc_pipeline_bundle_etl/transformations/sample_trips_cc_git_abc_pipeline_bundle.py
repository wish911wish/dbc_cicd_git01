from pyspark import pipelines as dp
from pyspark.sql.functions import col


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_trips_cc_git_abc_pipeline_bundle():
    return spark.read.table("samples.nyctaxi.trips")
